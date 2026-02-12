import os
from importlib.metadata import entry_points, version
import sys
import logging
import traceback

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.responses import Response

# from flask import Flask, jsonify
from psqlgraph import PsqlGraphDriver


from authutils import AuthError
from cdispyutils.log import get_handler
from cdispyutils.uwsgi import setup_user_harakiri
from dictionaryutils import DataDictionary, dictionary
from datamodelutils import models, validators, postgres_admin
from indexclient.client import IndexClient
from gen3authz.client.arborist.client import ArboristClient


import sheepdog
from sheepdog import logger
from sheepdog.config import settings
from sheepdog.errors import (
    APIError,
    setup_default_handlers,
    UnhealthyCheck,
)
from sheepdog.version_data import VERSION, COMMIT
from sheepdog.globals import dictionary_version, dictionary_commit

# recursion depth is increased for complex graph traversals
sys.setrecursionlimit(10000)
DEFAULT_ASYNC_WORKERS = 8


def load_modules(app: FastAPI = None) -> None:
    for ep in entry_points(group="sheepdog.modules"):
        logger.info(f"Loading module: {ep.name}")
        mod = ep.load()
        if app:
            init_app = getattr(mod, "init_app", None)
            if init_app:
                init_app(app)


# def app_register_blueprints(app):
#     # TODO: (jsm) deprecate the index endpoints on the root path,
#     # these are currently duplicated under /index (the ultimate
#     # path) for migration

#     app.url_map.strict_slashes = False

#     if "DICTIONARY_URL" in app.config:
#         url = app.config["DICTIONARY_URL"]
#         datadictionary = DataDictionary(url=url)
#     elif "PATH_TO_SCHEMA_DIR" in app.config:
#         datadictionary = DataDictionary(root_dir=app.config["PATH_TO_SCHEMA_DIR"])
#     else:
#         import gdcdictionary

#         datadictionary = gdcdictionary.gdcdictionary

#     dictionary.init(datadictionary)
#     from gen3datamodel import models as md
#     from gen3datamodel import validators as vd

#     models.init(md)
#     validators.init(vd)

#     # register the blueprint twice (at `/` and at `/v0/`). Flask requires the
#     # blueprint names to be unique, so rename it before registering the 2nd time
#     sheepdog_blueprint = sheepdog.create_blueprint("submission")
#     app.register_blueprint(sheepdog_blueprint, url_prefix="/v0/submission")
#     sheepdog_blueprint.name += "_legacy"
#     app.register_blueprint(sheepdog_blueprint, url_prefix="/submission")


# def db_init(app):
#     app.logger.info("Initializing PsqlGraph driver")
#     connect_args = {}
#     if app.config.get("PSQLGRAPH") and app.config["PSQLGRAPH"].get("sslmode"):
#         connect_args["sslmode"] = app.config["PSQLGRAPH"]["sslmode"]
#     app.db = PsqlGraphDriver(
#         host=app.config["PSQLGRAPH"]["host"],
#         user=app.config["PSQLGRAPH"]["user"],
#         password=app.config["PSQLGRAPH"]["password"],
#         database=app.config["PSQLGRAPH"]["database"],
#         set_flush_timestamps=True,
#         connect_args=connect_args,
#         isolation_level=app.config["PSQLGRAPH"].get(
#             "isolation_level", "READ_COMMITTED"
#         ),
#     )
#     if app.config.get("AUTO_MIGRATE_DATABASE"):
#         migrate_database(app)

#     app.logger.info("Initializing index client")
#     app.index_client = IndexClient(
#         app.config["INDEX_CLIENT"]["host"],
#         version=app.config["INDEX_CLIENT"]["version"],
#         auth=app.config["INDEX_CLIENT"]["auth"],
#     )

from functools import lru_cache
from starlette.requests import Request


class AppConfig:
    def __init__(self):
        self.AUTH_SUBMISSION_LIST = True
        self.USE_DBGAP = False
        self.IS_GDC = False
        self.AUTO_MIGRATE_DATABASE = True
        self.REQUIRE_FILE_INDEX_EXISTS = False
        self.FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "")
        self.PSQLGRAPH = {
            "host": os.getenv("PSQLGRAPH_HOST"),
            "user": os.getenv("PSQLGRAPH_USER"),
            "password": os.getenv("PSQLGRAPH_PASSWORD"),
            "database": os.getenv("PSQLGRAPH_DATABASE"),
            "sslmode": os.getenv("PSQLGRAPH_SSLMODE"),
            "isolation_level": os.getenv("PSQLGRAPH_ISOLATION", "READ_COMMITTED"),
        }
        self.INDEX_CLIENT = {
            "host": os.getenv("INDEX_CLIENT_HOST"),
            "version": os.getenv("INDEX_CLIENT_VERSION"),
            "auth": os.getenv("INDEX_CLIENT_AUTH"),
        }
        self.AUTH_NAMESPACE = "/" + os.getenv("AUTH_NAMESPACE", "").strip("/")
        self.USE_USER_HARAKIRI = os.getenv("USE_USER_HARAKIRI", "True") == "True"
        # more settings...


@lru_cache()
def get_config():
    return AppConfig()


def migrate_database(app):
    # hardcoded read role
    read_role = "peregrine"
    # postgres_admin.migrate_transaction_snapshots(app.state.db)
    if postgres_admin.check_version(app.state.db):
        logger.info("Haver version, returning")
        return
    try:
        postgres_admin.create_graph_tables(app.state.db, timeout=1)
    except Exception:
        if not postgres_admin.check_version(app.state.db):
            app.logger.exception("ERROR: Fail to migrate database")
            sys.exit(1)
        else:
            # if the version is already up to date, that means there is
            # another migration wins, so silently exit
            app.logger.info("The database version matches up. No need to do migration")
            return
    # check if such role exists
    # does this need to have a session?
    with app.state.db.session_scope() as session:
        session.connection(execution_options={"isolation_level": "READ COMMITTED"})
        # TODO: address B608
        r = [
            i
            for i in session.execute(
                "SELECT 1 FROM pg_roles WHERE rolname='{}'".format(read_role)  # nosec
            )
        ]
    if len(r) != 0:
        try:
            postgres_admin.grant_read_permissions_to_graph(app.state.db, read_role)
        except Exception:
            app.logger.warning(
                "Fail to grant read permission, continuing anyway. Details:"
            )
            traceback.print_exc()
            return


def app_init() -> FastAPI:
    logger.info("Initializing app")
    app = FastAPI(
        title="Sheepdog",
        version=version("sheepdog"),
        lifespan=lifespan,
    )
    logger.info("Ready to load modules")

    load_modules(app)

    return app


async def initialize_db(app):
    logger.info("Initializing PsqlGraph driver")
    # config = app.state.config
    logger.info(f"CONFIG {settings}")
    logger.info(f"CONFIG {settings.psqlgraph.get('host')}")
    connect_args = {}
    connect_args["sslmode"] = settings.psqlgraph.get("sslmode", "required")
    app.state.db = PsqlGraphDriver(
        host=settings.psqlgraph.get("host"),
        user=settings.psqlgraph.get("user"),
        password=settings.psqlgraph.get("password"),
        database=settings.psqlgraph.get("database"),
        set_flush_timestamps=True,
        connect_args=connect_args,
        isolation_level=settings.psqlgraph.get("isolation_level", "READ_COMMITTED"),
    )
    if settings.auto_migrate_database:
        logger.info("Auto migrating database")
        await migrate_database(app)

    logger.info("Initializing index client")
    app.state.index_client = IndexClient(
        settings.index_client.get("host"),
        version=settings.index_client.get("version"),
        auth=settings.index_client.get("auth"),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Startup: configure and initialize resources")

    # Setup config, logger, etc.
    settings.gen3_debug = os.environ.get("GEN3_DEBUG") == "True"
    # settings.AUTH_SUBMISSION_LIST = True
    # settings.USE_DBGAP = False
    # settings.IS_GDC = False
    settings.auto_migrate_database = os.environ.get("AUTO_MIGRATE_DATABASE", True)
    settings.require_file_index_exists = os.environ.get(
        "REQUIRE_FILE_INDEX_EXISTS", False
    )
    settings.auth_namespace = "/" + os.getenv("AUTH_NAMESPACE", "").strip("/")

    # level = logging.DEBUG if config["GEN3_DEBUG"] else logging.WARNING
    # get_logger(level)
    # app.state.logger = logger

    # # DataDictionary setup
    # datadictionary = get_datadictionary(config)
    # dictionary.init(datadictionary)
    # from gen3datamodel import models as md
    # from gen3datamodel import validators as vd
    # models.init(md)
    # validators.init(vd)

    # DB setup
    logger.info("Setting up DB")
    await initialize_db(app)

    # Arborist client
    arborist_url = os.environ.get("ARBORIST_URL", os.environ.get("ARBORIST"))
    if arborist_url:
        app.state.auth = ArboristClient(arborist_base_url=arborist_url)
    else:
        logger.info("Using default Arborist base URL")
        app.state.auth = ArboristClient()

    yield

    # Teardown if needed
    # logger.debug("Closing async client")
    # await app.async_client.aclose()


# # Setup logger
# app.logger.setLevel(
#     logging.DEBUG if (os.environ.get("GEN3_DEBUG") == "True") else logging.WARNING
# )
# app.logger.propagate = False
# while app.logger.handlers:
#     app.logger.removeHandler(app.logger.handlers[0])
# app.logger.addHandler(get_handler())

# setup_default_handlers(app)


# @app.route("/_status", methods=["GET"])
# def health_check():
#     """
#     Health check endpoint
#     ---
#     tags:
#       - system
#     responses:
#       200:
#         description: Healthy
#       default:
#         description: Unhealthy
#     """
#     with app.db.session_scope() as session:
#         try:
#             session.connection(execution_options={"isolation_level": "READ COMMITTED"})
#             session.execute("SELECT 1")
#         except Exception:
#             raise UnhealthyCheck("Unhealthy")

#     return "Healthy", 200

# @app.get("/_status", response_class=PlainTextResponse)
# async def health_check():
#     with app.db.session_scope() as session:
#         try:
#             session.connection(execution_options={"isolation_level": "READ COMMITTED"})
#             session.execute("SELECT 1")
#         except Exception:
#             raise UnhealthyCheck("Unhealthy")
#     return "Healthy"


# @app.route("/_version", methods=["GET"])
# def version():
#     """
#     Returns the version of Sheepdog
#     ---
#     tags:
#       - system
#     responses:
#       200:
#         description: successful operation
#     """
#     dictver = {"version": dictionary_version(), "commit": dictionary_commit()}
#     base = {"version": VERSION, "commit": COMMIT, "dictionary": dictver}

#     return jsonify(base), 200

# @app.get("/_version", response_class=JSONResponse)
# async def version():
#     dictver = {"version": dictionary_version(), "commit": dictionary_commit()}
#     base = {"version": VERSION, "commit": COMMIT, "dictionary": dictver}
#     return base

# @app.errorhandler(404)
# def page_not_found(e):
#     return jsonify(message=e.description), e.code


# @app.errorhandler(500)
# def server_error(e):
#     app.logger.exception(e)
#     return jsonify(message="internal server error"), 500


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


# app.register_error_handler(APIError, _log_and_jsonify_exception)

# app.register_error_handler(sheepdog.errors.APIError, _log_and_jsonify_exception)
# app.register_error_handler(AuthError, _log_and_jsonify_exception)

# @app.exception_handler(APIError)
# async def api_error_handler(request: Request, exc: APIError):
#     logger.exception(exc)
#     if hasattr(exc, "json") and exc.json:
#         return JSONResponse(status_code=exc.code, content=exc.json)
#     else:
#         return JSONResponse(status_code=exc.code, content={"message": getattr(exc, "message", str(exc))})

# @app.exception_handler(AuthError)
# async def auth_error_handler(request: Request, exc: AuthError):
#     logger.exception(exc)
#     return JSONResponse(status_code=getattr(exc, "code", 401), content={"message": str(exc)})

# @app.exception_handler(UnhealthyCheck)
# async def unhealthy_handler(request: Request, exc: UnhealthyCheck):
#     logger.exception(exc)
#     return JSONResponse(status_code=503, content={"message": "Unhealthy"})


def run_for_development(**kwargs) -> None:
    """
    Run the app for local development
    """
    # app.logger.setLevel(logging.INFO)

    import uvicorn

    uvicorn.run(
        "sheepdog.asgi:app",
        host=kwargs.get("host", "127.0.0.1"),
        port=kwargs.get("port", 8000),
        reload=kwargs.get("reload", True),
        log_level=kwargs.get("log_level", "info"),
    )
