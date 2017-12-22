import os
import sys

import cdis_oauth2client
from cdis_oauth2client import OAuth2Client, OAuth2Error
from cdisutils.log import get_handler
from flask import Flask, jsonify
from flask.ext.cors import CORS
from flask_sqlalchemy_session import flask_scoped_session
import gdcdatamodel
from indexclient.client import IndexClient as SignpostClient
from psqlgraph import PsqlGraphDriver
from userdatamodel.driver import SQLAlchemyDriver
from sheepdog.auth import AuthDriver
from sheepdog.config import LEGACY_MODE
from sheepdog.errors import APIError, setup_default_handlers, UnhealthyCheck
from sheepdog.version_data import VERSION, COMMIT, DICTVERSION, DICTCOMMIT
from dictionaryutils import DataDictionary

# recursion depth is increased for complex graph traversals
sys.setrecursionlimit(10000)
DEFAULT_ASYNC_WORKERS = 8

def get_parent(path):
    print(path)
    return path[0:path.rfind('/')]

'''
Test submission endpoints with generated dictionary pulled from S3.
Since requests does not support to handle local file, we mock the content of the dictionary by
instantiating DataDictionay with root_dir param
'''
PATH_TO_SCHEMA_DIR = get_parent(os.path.abspath(os.path.join(os.path.realpath(__file__), os.pardir))) + '/sheepdog/schemas'
datadictionary = DataDictionary(root_dir=PATH_TO_SCHEMA_DIR)

def app_register_blueprints(app):
    # TODO: (jsm) deprecate the index endpoints on the root path,
    # these are currently duplicated under /index (the ultimate
    # path) for migration
    v0 = '/v0'
    app.url_map.strict_slashes = False

    import sheepdog
    sheepdog_blueprint = sheepdog.create_blueprint(
        datadictionary, gdcdatamodel.models
    )

    app.register_blueprint(sheepdog_blueprint, url_prefix='/v0/submission')
    app.register_blueprint(cdis_oauth2client.blueprint, url_prefix=v0+'/oauth2')


def app_register_duplicate_blueprints(app):
    # TODO: (jsm) deprecate this v0 version under root endpoint.  This
    # root endpoint duplicates /v0 to allow gradual client migration
    blueprint2 = sheepdog.create_blueprint(
         datadictionary, models
    )

    app.register_blueprint(blueprint2, url_prefix='/submission')

def app_register_legacy_blueprints(app):
    legacy = '/legacy'
    app.register_blueprint(index.v0.blueprint, url_prefix=legacy)
    app.register_blueprint(index.v0.blueprint, url_prefix=legacy+'/index')
    app.register_blueprint(misc.v0.blueprint, url_prefix=legacy)
    app.register_blueprint(auth.v0.blueprint, url_prefix=legacy+'/auth')
    app.register_blueprint(manifest.v0.blueprint, url_prefix=legacy+'/manifest')
    app.register_blueprint(download.v0.blueprint, url_prefix=legacy+'/data')
    app.register_blueprint(submission.blueprint, url_prefix=legacy+'/submission')
    app.register_blueprint(slicing.v0.blueprint, url_prefix=legacy+'/slicing')


def app_register_v0_legacy_blueprints(app):
    v0_legacy = '/v0/legacy'
    app.register_blueprint(index.v0.blueprint, url_prefix=v0_legacy)
    app.register_blueprint(index.v0.blueprint, url_prefix=v0_legacy+'/index')
    app.register_blueprint(misc.v0.blueprint, url_prefix=v0_legacy)
    app.register_blueprint(auth.v0.blueprint, url_prefix=v0_legacy+'/auth')
    app.register_blueprint(manifest.v0.blueprint, url_prefix=v0_legacy+'/manifest')
    app.register_blueprint(download.v0.blueprint, url_prefix=v0_legacy+'/data')
    app.register_blueprint(submission.blueprint, url_prefix=v0_legacy+'/submission')
    app.register_blueprint(slicing.v0.blueprint, url_prefix=v0_legacy+'/slicing')


def async_pool_init(app):
    """Create and start an pool of workers for async tasks."""
    n_async_workers = (
        app.config
        .get('ASYNC', {})
        .get('N_WORKERS', DEFAULT_ASYNC_WORKERS)
    )
    app.async_pool = sheepdog.utils.scheduling.AsyncPool()
    app.async_pool.start(n_async_workers)


def db_init(app):
    app.logger.info('Initializing PsqlGraph driver')
    app.db = PsqlGraphDriver(
        host=app.config['PSQLGRAPH']['host'],
        user=app.config['PSQLGRAPH']['user'],
        password=app.config['PSQLGRAPH']['password'],
        database=app.config['PSQLGRAPH']['database'],
        set_flush_timestamps=True,
    )

    app.userdb = SQLAlchemyDriver(app.config['PSQL_USER_DB_CONNECTION'])
    flask_scoped_session(app.userdb.Session, app)

    app.oauth2 = OAuth2Client(**app.config['OAUTH2'])

    app.logger.info('Initializing Signpost driver')
    app.signpost = SignpostClient(
        app.config['SIGNPOST']['host'],
        version=app.config['SIGNPOST']['version'],
        auth=app.config['SIGNPOST']['auth'])
    try:
        app.logger.info('Initializing Auth driver')
        app.auth = AuthDriver(app.config["AUTH_ADMIN_CREDS"], app.config["INTERNAL_AUTH"])
    except Exception:
        app.logger.exception("Couldn't initialize auth, continuing anyway")


def es_init(app):
    app.logger.info('Initializing Elasticsearch driver')
    app.es = Elasticsearch([app.config["GDC_ES_HOST"]],
                           **app.config["GDC_ES_CONF"])


# Set CORS options on app configuration
def cors_init(app):
    accepted_headers = [
        'Content-Type',
        'X-Requested-With',
        'X-CSRFToken',
    ]
    CORS(app, resources={
        r"/*": {"origins": '*'},
        }, headers=accepted_headers, expose_headers=['Content-Disposition'])


def app_init(app):
    # Register duplicates only at runtime
    app.logger.info('Initializing app')
    # app_register_duplicate_blueprints(app)
    if LEGACY_MODE:
        app_register_legacy_blueprints(app)
        app_register_v0_legacy_blueprints(app)
    db_init(app)
    # exclude es init as it's not used yet
    # es_init(app)
    cors_init(app)
    try:
        app.secret_key = app.config['FLASK_SECRET_KEY']
    except KeyError:
        app.logger.error(
            'Secret key not set in config! Authentication will not work'
        )
    # slicing.v0.config(app)
    async_pool_init(app)
    app.logger.info('Initialization complete.')


app = Flask(__name__)

# Setup logger
app.logger.addHandler(get_handler())

setup_default_handlers(app)
app_register_blueprints(app)


@app.route('/_status', methods=['GET'])
def health_check():
    with app.db.session_scope() as session:
        try:
            session.execute('SELECT 1')
        except Exception:
            raise UnhealthyCheck('Unhealthy')

    return 'Healthy', 200

@app.route('/_version', methods=['GET'])
def version():
    dictver = {
        'version': DICTVERSION,
        'commit': DICTCOMMIT,
    }
    base = {
        'version': VERSION,
        'commit': COMMIT,
        'dictionary': dictver,
    }

    return jsonify(base), 200

@app.errorhandler(404)
def page_not_found(e):
    return jsonify(message=e.description), e.code


@app.errorhandler(500)
def server_error(e):
    app.logger.exception(e)
    return jsonify(message="internal server error"), 500


def _log_and_jsonify_exception(e):
    """
    Log an exception and return the jsonified version along with the code.

    This is the error handling mechanism for ``APIErrors`` and
    ``OAuth2Errors``.
    """
    app.logger.exception(e)
    if hasattr(e, 'json') and e.json:
        return jsonify(**e.json), e.code
    else:
        return jsonify(message=e.message), e.code


app.register_error_handler(APIError, _log_and_jsonify_exception)

import sheepdog.errors
app.register_error_handler(
    sheepdog.errors.APIError, _log_and_jsonify_exception
)
app.register_error_handler(OAuth2Error, _log_and_jsonify_exception)


def run_for_development(**kwargs):
    import logging
    app.logger.setLevel(logging.INFO)
    # app.config['PROFILE'] = True
    # from werkzeug.contrib.profiler import ProfilerMiddleware, MergeStream
    # f = open('profiler.log', 'w')
    # stream = MergeStream(sys.stdout, f)
    # app.wsgi_app = ProfilerMiddleware(app.wsgi_app, f, restrictions=[2000])

    for key in ["http_proxy", "https_proxy"]:
        if os.environ.get(key):
            del os.environ[key]
    app.config.from_object('sheepdog.dev_settings')

    kwargs['port'] = app.config['SHEEPDOG_PORT']
    kwargs['host'] = app.config['SHEEPDOG_HOST']

    try:
        app_init(app)
    except Exception:
        app.logger.exception(
            "Couldn't initialize application, continuing anyway"
        )
    app.run(**kwargs)
