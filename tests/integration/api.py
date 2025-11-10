# pylint: disable=superfluous-parens
# pylint: disable=redefined-outer-name
"""tests.api

Complimentary to conftest.py it sets up certain functionality
"""

import sys

from cdispyutils.log import get_handler
from flask import Flask, jsonify
from indexclient.client import IndexClient
from psqlgraph import PsqlGraphDriver

import sheepdog
from sheepdog.errors import APIError, setup_default_handlers, UnhealthyCheck
from sheepdog.version_data import VERSION, COMMIT
from sheepdog.globals import dictionary_version, dictionary_commit

# recursion depth is increased for complex graph traversals
sys.setrecursionlimit(10000)
DEFAULT_ASYNC_WORKERS = 8


def db_init(app):
    app.logger.info("Initializing PsqlGraph driver")
    app.db = PsqlGraphDriver(
        host=app.config["PSQLGRAPH"]["host"],
        user=app.config["PSQLGRAPH"]["user"],
        password=app.config["PSQLGRAPH"]["password"],
        database=app.config["PSQLGRAPH"]["database"],
        set_flush_timestamps=True,
    )

    app.logger.info("Initializing Indexd driver")
    app.index_client = IndexClient(
        app.config["INDEX_CLIENT"]["host"],
        version=app.config["INDEX_CLIENT"]["version"],
        auth=app.config["INDEX_CLIENT"]["auth"],
    )


def app_init(app):
    # TODO can we just use the real app init?

    # Register duplicates only at runtime
    app.logger.info("Initializing app")

    app.config["REQUIRE_FILE_INDEX_EXISTS"] = (
        # If True, enforce indexd record exists before file node registration
        app.config.get("REQUIRE_FILE_INDEX_EXISTS", False)
    )

    app.url_map.strict_slashes = False
    db_init(app)
    try:
        app.secret_key = app.config["FLASK_SECRET_KEY"]
    except KeyError:
        app.logger.error("Secret key not set in config! Authentication will not work")

    v0 = "/v0"
    try:
        sheepdog_blueprint = sheepdog.create_blueprint("submission")
        app.register_blueprint(sheepdog_blueprint, url_prefix=v0 + "/submission")
        sheepdog_blueprint.name += "_legacy"
        app.register_blueprint(sheepdog_blueprint, url_prefix="/submission")
    except (ValueError, AssertionError):
        app.logger.info("Blueprint is already registered!!!")


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

    This is the error handling mechanism for ``APIErrors``.
    """
    app.logger.exception(e)
    if hasattr(e, "json") and e.json:
        return jsonify(**e.json), e.code
    return jsonify(message=e.message), e.code


app.register_error_handler(APIError, _log_and_jsonify_exception)
