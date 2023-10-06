from sheepdog.api import app, app_init
from os import environ
import confighelper

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
    "host": environ.get("SIGNPOST_HOST") or "http://indexd-service",
    "version": "v0",
    "auth": ("gdcapi", conf_data.get("indexd_password", "{{indexd_password}}")),
}
config["INDEX_CLIENT"] = {
    "host": environ.get("INDEX_CLIENT_HOST") or "http://indexd-service",
    "version": "v0",
    "auth": ("gdcapi", conf_data.get("indexd_password", "{{indexd_password}}")),
}
config["FAKE_AUTH"] = False
config["PSQLGRAPH"] = {
    "host": conf_data["db_host"],
    "user": conf_data["db_username"],
    "password": conf_data["db_password"],
    "database": conf_data["db_database"],
}

config["FLASK_SECRET_KEY"] = conf_data.get("gdcapi_secret_key", "{{gdcapi_secret_key}}")
config["PSQL_USER_DB_CONNECTION"] = "postgresql://%s:%s@%s:5432/%s" % tuple(
    [
        conf_data.get(key, key)
        for key in ["fence_username", "fence_password", "fence_host", "fence_database"]
    ]
)

config["USER_API"] = "https://%s/user" % conf_data["hostname"]  # for use by authutils
# use the USER_API URL instead of the public issuer URL to accquire JWT keys
config["FORCE_ISSUER"] = True
config["DICTIONARY_URL"] = environ.get(
    "DICTIONARY_URL",
    "https://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json",
)

app_init(app)
application = app
application.debug = environ.get("GEN3_DEBUG") == "True"
