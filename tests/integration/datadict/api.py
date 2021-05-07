# pylint: disable=superfluous-parens
# pylint: disable=redefined-outer-name
"""tests.api

Complimentary to conftest.py it sets up certain functionality
"""

import sqlite3
import sys
import os
import importlib

import cdis_oauth2client
from cdis_oauth2client import OAuth2Client, OAuth2Error
from cdispyutils.log import get_handler
from flask import Flask, jsonify
from flask_sqlalchemy_session import flask_scoped_session
from indexclient.client import IndexClient
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from psqlgraph import PsqlGraphDriver

import sheepdog
from sheepdog.errors import APIError, setup_default_handlers, UnhealthyCheck
from sheepdog.version_data import VERSION, COMMIT
from sheepdog.globals import dictionary_version, dictionary_commit

# recursion depth is increased for complex graph traversals
sys.setrecursionlimit(10000)
DEFAULT_ASYNC_WORKERS = 8


def app_register_blueprints(app):
    # TODO: (jsm) deprecate the index endpoints on the root path,
    # these are currently duplicated under /index (the ultimate
    # path) for migration
    v0 = "/v0"
    app.url_map.strict_slashes = False

    app.register_blueprint(cdis_oauth2client.blueprint, url_prefix=v0 + "/oauth2")


def db_init(app):
    app.logger.info("Initializing PsqlGraph driver")
    app.db = PsqlGraphDriver(
        host=app.config["PSQLGRAPH"]["host"],
        user=app.config["PSQLGRAPH"]["user"],
        password=app.config["PSQLGRAPH"]["password"],
        database=app.config["PSQLGRAPH"]["database"],
        set_flush_timestamps=True,
    )

    app.oauth2 = OAuth2Client(**app.config["OAUTH2"])

    app.logger.info("Initializing Indexd driver")
    app.index_client = IndexClient(
        app.config["INDEX_CLIENT"]["host"],
        version=app.config["INDEX_CLIENT"]["version"],
        auth=app.config["INDEX_CLIENT"]["auth"],
    )


def app_init(app):
    # Register duplicates only at runtime
    app.logger.info("Initializing app")

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
    sheepdog_blueprint = sheepdog.create_blueprint("submission")

    try:
        app.register_blueprint(sheepdog_blueprint, url_prefix="/v0/submission")
    except AssertionError:
        app.logger.info("Blueprint is already registered!!!")

    app.node_authz_entity_name = os.environ.get("AUTHZ_ENTITY_NAME", None)
    app.node_authz_entity = None
    app.subject_entity  = None
    if app.node_authz_entity_name:
        full_module_name = "datamodelutils.models"
        mymodule = importlib.import_module(full_module_name)
        for i in dir(mymodule):
            app.logger.warn(i)
            if i.lower() == "person":
                attribute = getattr(mymodule, i)
                app.subject_entity  = attribute
            if i.lower() == app.node_authz_entity_name.lower():
                attribute = getattr(mymodule, i)
                app.node_authz_entity = attribute


app = Flask(__name__)

# Setup logger
app.logger.addHandler(get_handler())

setup_default_handlers(app)


@app.route("/_status", methods=["GET"])
def health_check():
    with app.db.session_scope() as session:
        try:
            session.execute("SELECT 1")
        except Exception:
            raise UnhealthyCheck("Unhealthy")

    return "Healthy", 200


@app.route("/_version", methods=["GET"])
def version():
    dictver = {"version": dictionary_version(), "commit": dictionary_commit()}
    base = {"version": VERSION, "commit": COMMIT, "dictionary": dictver}

    return jsonify(base), 200


def _log_and_jsonify_exception(e):
    """
    Log an exception and return the jsonified version along with the code.

    This is the error handling mechanism for ``APIErrors`` and
    ``OAuth2Errors``.
    """
    app.logger.exception(e)
    if hasattr(e, "json") and e.json:
        return jsonify(**e.json), e.code
    return jsonify(message=e.message), e.code


app.register_error_handler(APIError, _log_and_jsonify_exception)

app.register_error_handler(APIError, _log_and_jsonify_exception)
app.register_error_handler(OAuth2Error, _log_and_jsonify_exception)

OLD_SQLITE = sqlite3.sqlite_version_info < (3, 7, 16)

INDEX_HOST = "index.sq3"
ALIAS_HOST = "alias.sq3"


INDEX_TABLES = {
    "index_record": [
        (0, "did", "VARCHAR", 1, None, 1),
        (1, "rev", "VARCHAR", 0, None, 0),
        (2, "form", "VARCHAR", 0, None, 0),
        (3, "size", "INTEGER", 0, None, 0),
    ],
    "index_record_hash": [
        (0, "did", "VARCHAR", 1, None, 1),
        (1, "hash_type", "VARCHAR", 1, None, 1 if OLD_SQLITE else 2),
        (2, "hash_value", "VARCHAR", 0, None, 0),
    ],
    "index_record_url": [
        (0, "did", "VARCHAR", 1, None, 1),
        (1, "url", "VARCHAR", 1, None, 1 if OLD_SQLITE else 2),
    ],
}


# pulled from indexd/tests/test_setup.py
ALIAS_TABLES = {
    "alias_record": [
        (0, "name", "VARCHAR", 1, None, 1),
        (1, "rev", "VARCHAR", 0, None, 0),
        (2, "size", "INTEGER", 0, None, 0),
        (3, "release", "VARCHAR", 0, None, 0),
        (4, "metastring", "VARCHAR", 0, None, 0),
        (5, "keeper_authority", "VARCHAR", 0, None, 0),
    ],
    "alias_record_hash": [
        (0, "name", "VARCHAR", 1, None, 1),
        (1, "hash_type", "VARCHAR", 1, None, 1 if OLD_SQLITE else 2),
        (2, "hash_value", "VARCHAR", 0, None, 0),
    ],
    "alias_record_host_authority": [
        (0, "name", "VARCHAR", 1, None, 1),
        (1, "host", "VARCHAR", 1, None, 1 if OLD_SQLITE else 2),
    ],
}


def setup_sqlite3_index_tables():
    """Setup the SQLite3 index database."""

    SQLAlchemyIndexDriver("sqlite:///index.sq3")

    with sqlite3.connect(INDEX_HOST) as conn:
        connection = conn.execute(
            """
            SELECT name FROM sqlite_master WHERE type = 'table'
        """
        )

        tables = [i[0] for i in connection]

        for table in INDEX_TABLES:
            assert table in tables, "{table} not created".format(table=table)

        for table, _ in INDEX_TABLES.items():
            # NOTE PRAGMA's don't work with parameters...
            connection = conn.execute(
                """
                PRAGMA table_info ('{table}')
            """.format(
                    table=table
                )
            )


def setup_sqlite3_alias_tables():
    """Setup the SQLite3 alias database."""

    SQLAlchemyAliasDriver("sqlite:///alias.sq3")

    with sqlite3.connect(ALIAS_HOST) as conn:
        connection = conn.execute(
            """
            SELECT name FROM sqlite_master WHERE type = 'table'
        """
        )

        tables = [i[0] for i in connection]

        for table in ALIAS_TABLES:
            assert table in tables, "{table} not created".format(table=table)

        for table, _ in ALIAS_TABLES.items():
            # NOTE PRAGMA's don't work with parameters...
            connection = conn.execute(
                """
                PRAGMA table_info ('{table}')
            """.format(
                    table=table
                )
            )


def setup_sqlite3_auth_tables(username, password):
    """Setup the SQLite3 auth database."""
    auth_driver = SQLAlchemyAuthDriver("sqlite:///auth.sq3")
    try:
        auth_driver.add(username, password)
    except Exception as error:
        print("Unable to create auth tables")
        print(error)


def indexd_init(username, password):
    setup_sqlite3_index_tables()
    setup_sqlite3_alias_tables()
    setup_sqlite3_auth_tables(username, password)
