import os
import sys

from flask import Flask, jsonify
from psqlgraph import PsqlGraphDriver
from sqlalchemy import MetaData, Table

from authutils.oauth2 import client as oauth2_client
from authutils.oauth2.client import blueprint as oauth2_blueprint
from authutils import AuthError
from cdispyutils.log import get_handler
from dictionaryutils import DataDictionary, dictionary
from datamodelutils import models, validators, postgres_admin


from indexclient.client import IndexClient as SignpostClient

import sheepdog
from sheepdog.errors import (
    APIError,
    setup_default_handlers,
    UnhealthyCheck,
    NotFoundError,
    InternalError,
)
from sheepdog.version_data import VERSION, COMMIT
from sheepdog.globals import dictionary_version, dictionary_commit

# recursion depth is increased for complex graph traversals
sys.setrecursionlimit(10000)
DEFAULT_ASYNC_WORKERS = 8


def app_register_blueprints(app):
    # TODO: (jsm) deprecate the index endpoints on the root path,
    # these are currently duplicated under /index (the ultimate
    # path) for migration

    app.url_map.strict_slashes = False

    if "DICTIONARY_URL" in app.config:
        url = app.config["DICTIONARY_URL"]
        datadictionary = DataDictionary(url=url)
    elif "PATH_TO_SCHEMA_DIR" in app.config:
        datadictionary = DataDictionary(root_dir=app.config["PATH_TO_SCHEMA_DIR"])
    else:
        import gdcdictionary

        datadictionary = gdcdictionary.gdcdictionary

    dictionary.init(datadictionary)
    from gdcdatamodel import models as md
    from gdcdatamodel import validators as vd

    models.init(md)
    validators.init(vd)
    sheepdog_blueprint = sheepdog.create_blueprint("submission")

    v0 = "/v0"
    app.register_blueprint(sheepdog_blueprint, url_prefix=v0 + "/submission")
    app.register_blueprint(sheepdog_blueprint, url_prefix="/submission")
    app.register_blueprint(oauth2_blueprint.blueprint, url_prefix=v0 + "/oauth2")
    app.register_blueprint(oauth2_blueprint.blueprint, url_prefix="/oauth2")


def db_init(app):
    app.logger.info("Initializing PsqlGraph driver")
    app.db = PsqlGraphDriver(
        host=app.config["PSQLGRAPH"]["host"],
        user=app.config["PSQLGRAPH"]["user"],
        password=app.config["PSQLGRAPH"]["password"],
        database=app.config["PSQLGRAPH"]["database"],
        set_flush_timestamps=True,
    )
    if app.config.get("AUTO_MIGRATE_DATABASE"):
        migrate_database(app)

    app.oauth_client = oauth2_client.OAuthClient(**app.config["OAUTH2"])

    app.logger.info("Initializing Signpost driver")
    app.signpost = SignpostClient(
        app.config["SIGNPOST"]["host"],
        version=app.config["SIGNPOST"]["version"],
        auth=app.config["SIGNPOST"]["auth"],
    )


def migrate_database(app):
    postgres_admin.migrate_transaction_snapshots(app.db, read_role)
    if postgres_admin.check_version(app.db):
        return
    try:
        postgres_admin.create_graph_tables(app.db, timeout=1)
    except Exception:
        if not postgres_admin.check_version(app.db):
            app.logger.exception("ERROR: Fail to migrate database")
            sys.exit(1)
        else:
            # if the version is already up to date, that means there is
            # another migration wins, so silently exit
            app.logger.info("The database version matches up. No need to do migration")
            return
    # hardcoded read role
    read_role = "peregrine"
    # check if such role exists
    with app.db.session_scope() as session:
        r = [
            i
            for i in session.execute(
                "SELECT 1 FROM pg_roles WHERE rolname='{}'".format(read_role)
            )
        ]
    if len(r) != 0:
        try:
            postgres_admin.grant_read_permissions_to_graph(app.db, read_role)
        except Exception:
            app.logger.warn("Fail to grant read permission, continuing anyway")
            return


def app_init(app):
    # Register duplicates only at runtime
    app.logger.info("Initializing app")

    # explicit options set for compatibility with gdc's api
    app.config["USE_SIGNPOST"] = False
    app.config["AUTH_SUBMISSION_LIST"] = True
    app.config["USE_DBGAP"] = False
    app.config["IS_GDC"] = False

    # default settings
    app.config["AUTO_MIGRATE_DATABASE"] = app.config.get("AUTO_MIGRATE_DATABASE", True)
    app.config["REQUIRE_FILE_INDEX_EXISTS"] = (
        # If True, enforce indexd record exists before file node registration
        app.config.get("REQUIRE_FILE_INDEX_EXISTS", False)
    )

    app_register_blueprints(app)
    db_init(app)
    # exclude es init as it's not used yet
    # es_init(app)
    try:
        app.secret_key = app.config["FLASK_SECRET_KEY"]
    except KeyError:
        app.logger.error("Secret key not set in config! Authentication will not work")


app = Flask(__name__)


# Setup logger
app.logger.addHandler(get_handler())

setup_default_handlers(app)


@app.route("/_status", methods=["GET"])
def health_check():
    """
    Health check endpoint
    ---
    tags:
      - system
    responses:
      200:
        description: Healthy
      default:
        description: Unhealthy
    """
    with app.db.session_scope() as session:
        try:
            session.execute("SELECT 1")
        except Exception:
            raise UnhealthyCheck("Unhealthy")

    return "Healthy", 200


@app.route("/_version", methods=["GET"])
def version():
    """
    Returns the version of Sheepdog
    ---
    tags:
      - system
    responses:
      200:
        description: successful operation
    """
    dictver = {"version": dictionary_version(), "commit": dictionary_commit()}
    base = {"version": VERSION, "commit": COMMIT, "dictionary": dictver}

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
    ``AuthError``.
    """
    app.logger.exception(e)
    if hasattr(e, "json") and e.json:
        return jsonify(**e.json), e.code
    else:
        return jsonify(message=e.message), e.code


app.register_error_handler(APIError, _log_and_jsonify_exception)

app.register_error_handler(sheepdog.errors.APIError, _log_and_jsonify_exception)
app.register_error_handler(AuthError, _log_and_jsonify_exception)


def run_for_development(**kwargs):
    # app.logger.setLevel(logging.INFO)

    for key in ["http_proxy", "https_proxy"]:
        if os.environ.get(key):
            del os.environ[key]
    app.config.from_object("sheepdog.dev_settings")

    kwargs["port"] = app.config["SHEEPDOG_PORT"]
    kwargs["host"] = app.config["SHEEPDOG_HOST"]

    try:
        app_init(app)
    except Exception:
        app.logger.exception("Couldn't initialize application, continuing anyway")
    app.run(**kwargs)
