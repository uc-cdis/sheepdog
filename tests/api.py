import sqlite3
import sys
import cdis_oauth2client

from flask import Flask, jsonify
from flask.ext.cors import CORS
from flask_sqlalchemy_session import flask_scoped_session
from psqlgraph import PsqlGraphDriver

from cdis_oauth2client import OAuth2Client, OAuth2Error
from cdispyutils.log import get_handler
from indexclient.client import IndexClient
from userdatamodel.driver import SQLAlchemyDriver

from sheepdog.auth import AuthDriver
from sheepdog.errors import APIError, setup_default_handlers, UnhealthyCheck
from sheepdog.version_data import VERSION, COMMIT, DICTVERSION, DICTCOMMIT

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver


# recursion depth is increased for complex graph traversals
sys.setrecursionlimit(10000)
DEFAULT_ASYNC_WORKERS = 8

def app_register_blueprints(app):
    # TODO: (jsm) deprecate the index endpoints on the root path,
    # these are currently duplicated under /index (the ultimate
    # path) for migration
    v0 = '/v0'
    app.url_map.strict_slashes = False

    app.register_blueprint(cdis_oauth2client.blueprint, url_prefix=v0+'/oauth2')


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

    app.logger.info('Initializing Indexd driver')
    app.signpost = IndexClient(
        app.config['SIGNPOST']['host'],
        version=app.config['SIGNPOST']['version'],
        auth=app.config['SIGNPOST']['auth'])
    try:
        app.logger.info('Initializing Auth driver')
        app.auth = AuthDriver(app.config["AUTH_ADMIN_CREDS"], app.config["INTERNAL_AUTH"])
    except Exception:
        app.logger.exception("Couldn't initialize auth, continuing anyway")


def app_init(app):
    # Register duplicates only at runtime
    app.logger.info('Initializing app')
    app_register_blueprints(app)
    db_init(app)
    # exclude es init as it's not used yet
    # es_init(app)
    try:
        app.secret_key = app.config['FLASK_SECRET_KEY']
    except KeyError:
        app.logger.error(
            'Secret key not set in config! Authentication will not work'
        )


app = Flask(__name__)

# Setup logger
app.logger.addHandler(get_handler())

setup_default_handlers(app)


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

app.register_error_handler(
    APIError, _log_and_jsonify_exception
)
app.register_error_handler(OAuth2Error, _log_and_jsonify_exception)

OLD_SQLITE = sqlite3.sqlite_version_info < (3, 7, 16)

INDEX_HOST = 'index.sq3'
ALIAS_HOST = 'alias.sq3'

INDEX_TABLES = {
    'index_record': [
        (0, u'did', u'VARCHAR', 1, None, 1),
        (1, u'rev', u'VARCHAR', 0, None, 0),
        (2, u'form', u'VARCHAR', 0, None, 0),
        (3, u'size', u'INTEGER', 0, None, 0),
    ],
    'index_record_hash': [
        (0, u'did', u'VARCHAR', 1, None, 1),
        (1, u'hash_type', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
        (2, u'hash_value', u'VARCHAR', 0, None, 0),
    ],
    'index_record_url': [
        (0, u'did', u'VARCHAR', 1, None, 1),
        (1, u'url', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
    ],
}


# pulled from indexd/tests/test_setup.py
ALIAS_TABLES = {
    'alias_record': [
        (0, u'name', u'VARCHAR', 1, None, 1),
        (1, u'rev', u'VARCHAR', 0, None, 0),
        (2, u'size', u'INTEGER', 0, None, 0),
        (3, u'release', u'VARCHAR', 0, None, 0),
        (4, u'metastring', u'VARCHAR', 0, None, 0),
        (5, u'keeper_authority', u'VARCHAR', 0, None, 0),
    ],
    'alias_record_hash': [
        (0, u'name', u'VARCHAR', 1, None, 1),
        (1, u'hash_type', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
        (2, u'hash_value', u'VARCHAR', 0, None, 0)
    ],
    'alias_record_host_authority': [
        (0, u'name', u'VARCHAR', 1, None, 1),
        (1, u'host', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
    ],
}

INDEX_CONFIG = {
    'driver': SQLAlchemyIndexDriver('sqlite:///index.sq3'),
}

ALIAS_CONFIG = {
    'driver': SQLAlchemyAliasDriver('sqlite:///alias.sq3'),
}

def setup_sqlite3_index_tables():
    """Setup the SQLite3 index database."""

    SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with sqlite3.connect(INDEX_HOST) as conn:
        c = conn.execute('''
            SELECT name FROM sqlite_master WHERE type = 'table'
        ''')

        tables = [i[0] for i in c]

        for table in INDEX_TABLES:
            assert table in tables, '{table} not created'.format(table=table)

        for table, schema in INDEX_TABLES.items():
            # NOTE PRAGMA's don't work with parameters...
            c = conn.execute('''
                PRAGMA table_info ('{table}')
            '''.format(table=table))

def setup_sqlite3_alias_tables():
    """Setup the SQLite3 alias database."""

    SQLAlchemyAliasDriver('sqlite:///alias.sq3')

    with sqlite3.connect(ALIAS_HOST) as conn:
        c = conn.execute('''
            SELECT name FROM sqlite_master WHERE type = 'table'
        ''')

        tables = [i[0] for i in c]

        for table in ALIAS_TABLES:
            assert table in tables, '{table} not created'.format(table=table)

        for table, schema in ALIAS_TABLES.items():
            # NOTE PRAGMA's don't work with parameters...
            c = conn.execute('''
                PRAGMA table_info ('{table}')
            '''.format(table=table))

def setup_sqlite3_auth_tables(username, password):
    """Setup the SQLite3 auth database."""
    auth_driver = SQLAlchemyAuthDriver('sqlite:///auth.sq3')
    try:
        auth_driver.add(username, password)
        print('User {} created'.format(username))
    except Exception as e:
        print('oh no')

def indexd_init(username, password):
    setup_sqlite3_index_tables()
    setup_sqlite3_alias_tables()
    setup_sqlite3_auth_tables(username, password)
