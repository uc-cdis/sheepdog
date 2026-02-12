"""
Pydantic Settings configuration for sheepdog.

Configuration is loaded from a JSON file (config.json or
path specified in SHEEPDOG_CONFIG_PATH).

This replaces the Flask bin.settings dict for FastAPI migration.
"""

import os
from cdislogging import get_logger
from os import environ
from pydantic_settings import BaseSettings
import bin.confighelper as confighelper

logger = get_logger(__name__)

# ======================================================================
# Legacy Mode

# LEGACY_MODE specifies whether the API will run in Legacy Mode. This
# mode will affect which mapping and index is used is used.
LEGACY_MODE = os.environ.get("GDC_API_LEGACY_MODE", "").lower() == "true"


if LEGACY_MODE:
    logger.info(
        "Running in LEGACY mode. The Elasticsearch 'GDC_ES_LEGACY_INDEX' "
        "environment variable and the legacy mapping will be used. "
    )
else:
    logger.info(
        "Running in ACTIVE mode. The Elasticsearch 'GDC_ES_INDEX' "
        "environment variable and the active mapping will be used. "
    )

APP_NAME = "sheepdog"


def load_json(file_name):
    return confighelper.load_json(file_name, APP_NAME)


conf_data = load_json("creds.json")


class Settings(BaseSettings):

    # explicit options set for compatibility with gdc's api
    auth_submission_list: bool = True
    use_dbgap: bool = False
    is_gdc: bool = False

    require_file_index_exists: bool = False
    auto_migrate_database: bool = True
    auth_namespace: str = ""

    arborist_url: str = "http://arborist-service/"
    index_client: dict = {
        "host": os.environ.get("INDEX_CLIENT_HOST") or "http://indexd-service",
        "version": "v0",
        "auth": (
            environ.get("INDEXD_USER", "gdcapi"),
            environ.get("INDEXD_PASS")
            or conf_data.get("indexd_password", "{{indexd_password}}"),
        ),
    }
    psqlgraph: dict = {
        "host": conf_data.get("db_host", os.environ.get("PGHOST", "localhost")),
        "user": conf_data.get("db_username", os.environ.get("PGUSER", "sheepdog")),
        "password": conf_data.get(
            "db_password", os.environ.get("PGPASSWORD", "sheepdog")
        ),
        "database": conf_data.get("db_database", os.environ.get("PGDB", "sheepdog")),
        "sslmode": "require",
    }
    flask_secret_key: str = conf_data.get("gdcapi_secret_key", "{{gdcapi_secret_key}}")
    psql_user_db_connection: str = (
        "postgresql://{username}:{password}@{host}:5432/{database}".format(
            username=conf_data.get(
                "fence_username", os.environ.get("FENCE_DB_USER", "fence")
            ),
            password=conf_data.get(
                "fence_password", os.environ.get("FENCE_DB_PASS", "fence")
            ),
            host=conf_data.get(
                "fence_host", os.environ.get("FENCE_DB_HOST", "localhost")
            ),
            database=conf_data.get(
                "fence_database", os.environ.get("FENCE_DB_DATABASE", "fence")
            ),
        )
    )
    user_api: str = "https://{hostname}/user".format(
        hostname=conf_data.get("hostname", os.environ.get("CONF_HOSTNAME", "localhost"))
    )
    force_issuer: bool = True
    dictionary_url: str = os.environ.get(
        "DICTIONARY_URL",
        "https://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json",
    )
    gen3_debug: bool = os.environ.get("GEN3_DEBUG") == "True"

    # Local settings


settings = Settings()
