from sheepdog.api import app, app_init
import os
from . import confighelper

APP_NAME = "sheepdog"


def load_json(file_name):
    return confighelper.load_json(file_name, APP_NAME)


conf_data = load_json("creds.json")
config = app.config

config["AUTH"] = "https://auth.service.consul:5000/v3/"
config["AUTH_ADMIN_CREDS"] = None
config["INTERNAL_AUTH"] = None

# ARBORIST deprecated, replaced by ARBORIST_URL
# ARBORIST_URL is initialized in app_init() directly
config["ARBORIST"] = "http://arborist-service/"

# Signpost: deprecated, replaced by index client.
config["SIGNPOST"] = {
    "host": os.environ.get("SIGNPOST_HOST", "http://indexd-service"),
    "version": "v0",
    "auth": ("gdcapi", conf_data.get("indexd_password", "{{indexd_password}}")),
}
config["INDEX_CLIENT"] = {
    "host": os.environ.get("INDEX_CLIENT_HOST") or "http://indexd-service",
    "version": "v0",
    "auth": ("gdcapi", conf_data.get("indexd_password", "{{indexd_password}}")),
}
config["FAKE_AUTH"] = False
config["PSQLGRAPH"] = {
    "host": conf_data.get("db_host", os.environ.get("PGHOST", "localhost")),
    "user": conf_data.get("db_username", os.environ.get("PGUSER", "sheepdog")),
    "password": conf_data.get("db_password", os.environ.get("PGPASSWORD", "sheepdog")),
    "database": conf_data.get("db_database", os.environ.get("PGDATABASE", "sheepdog")),
}

config["FLASK_SECRET_KEY"] = conf_data.get("gdcapi_secret_key", "{{gdcapi_secret_key}}")

fence_username = conf_data.get(
    "fence_username", os.environ.get("FENCE_DB_USERNAME", "fence")
)
fence_password = conf_data.get(
    "fence_password", os.environ.get("FENCE_DB_PASSWORD", "fence")
)
fence_host = conf_data.get("fence_host", os.environ.get("FENCE_DB_HOST", "localhost"))
fence_database = conf_data.get(
    "fence_database", os.environ.get("FENCE_DB_DATABASE", "fence")
)
config["PSQL_USER_DB_CONNECTION"] = "postgresql://%s:%s@%s:5432/%s" % (
    fence_username,
    fence_password,
    fence_host,
    fence_database,
)

config["USER_API"] = "https://%s/user" % conf_data.get(
    "hostname", os.environ.get("CONF_HOSTNAME", "localhost")
)  # for use by authutils
# use the USER_API URL instead of the public issuer URL to accquire JWT keys
config["FORCE_ISSUER"] = True
config["DICTIONARY_URL"] = os.environ.get(
    "DICTIONARY_URL",
    "https://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json",
)

app_init(app)
application = app
application.debug = os.environ.get("GEN3_DEBUG") == "True"
