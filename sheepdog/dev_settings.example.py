import os
from boto.s3.connection import OrdinaryCallingFormat
from os import environ as env

from .config import LEGACY_MODE

# Signpost
SIGNPOST = {
   'host': env.get('SIGNPOST_HOST', 'http://localhost:8888'),
   'version': 'v0',
   'auth': None}

# Auth
AUTH = 'https://gdc-portal.nci.nih.gov/auth/keystone/v3/'
INTERNAL_AUTH = env.get('INTERNAL_AUTH', 'https://gdc-portal.nci.nih.gov/auth/')

AUTH_ADMIN_CREDS = {
    'domain_name': env.get('KEYSTONE_DOMAIN'),
    'username': env.get('KEYSTONE_USER'),
    'password': env.get('KEYSTONE_PASSWORD'),
    'auth_url': env.get('KEYSTONE_AUTH_URL'),
    'user_domain_name': env.get('KEYSTONE_DOMAIN')}

# Storage
CLEVERSAFE_HOST = env.get('CLEVERSAFE_HOST', 'cleversafe.service.consul')


STORAGE = {"s3": {
    "keys": {
        "cleversafe.service.consul": {
            "access_key": os.environ.get('CLEVERSAFE_ACCESS_KEY'),
            'secret_key': os.environ.get('CLEVERSAFE_SECRET_KEY')},
        "localhost": {
            "access_key": os.environ.get('CLEVERSAFE_ACCESS_KEY'),
            'secret_key': os.environ.get('CLEVERSAFE_SECRET_KEY')},
    }, "kwargs": {
        'cleversafe.service.consul': {
            'host': 'cleversafe.service.consul',
            "is_secure": False,
            "calling_format": OrdinaryCallingFormat()},
        'localhost': {
            'host': 'localhost',
            "is_secure": False,
            "calling_format": OrdinaryCallingFormat()},
    }}}

SUBMISSION = {
    "bucket": 'test_submission',
    "host": CLEVERSAFE_HOST,
}



# Postgres
PSQLGRAPH = {
    'host': os.getenv("GDC_PG_HOST", "localhost"),
    'user': os.getenv("GDC_PG_USER", "test"),
    'password': os.getenv("GDC_PG_PASSWORD", "test"),
    'database': os.getenv("GDC_PG_DBNAME", "automated_test")
}

PSQL_USER_DB_NAME = 'userapi'
PSQL_USER_DB_USERNAME = ''
PSQL_USER_DB_PASSWORD = ''
PSQL_USER_DB_HOST = ''

PSQL_USER_DB_CONNECTION = "postgresql://{name}:{password}@{host}/{db}".format(
    name=PSQL_USER_DB_USERNAME, password=PSQL_USER_DB_PASSWORD, host=PSQL_USER_DB_HOST, db=PSQL_USER_DB_NAME
)

GDC_PORTAL_ENDPOINT = os.getenv("GDC_PORTAL_ENDPOINT", 'http://gdc-portal.nci.nih.gov:*')

# API server
GDC_API_HOST = os.getenv("GDC_API_HOST", "localhost")
GDC_API_PORT = int(os.getenv("GDC_API_PORT", "5000"))

# ES settings
_default_index = "gdc_legacy_test" if LEGACY_MODE else "gdc_test"
GDC_ES_INDEX = os.getenv("GDC_ES_INDEX", _default_index)
GDC_ES_HOST = os.getenv("GDC_ES_HOST", "localhost")
GDC_ES_CONF = {"port": int(os.getenv("GDC_ES_PORT", "9200"))}
gdc_es_user = os.getenv("GDC_ES_USER", None)
gdc_es_pass = os.getenv("GDC_ES_PASS", None)
if gdc_es_user is not None and gdc_es_pass is not None:
    GDC_ES_CONF["http_auth"] = (gdc_es_user, gdc_es_pass)
GDC_ES_STATS_INDEX = os.getenv("GDC_ES_STATS_INDEX", "download_stats")

GEO_API = 'http://geoserver.service.consul'

# Slicing settings
SLICING = {
    'host': 'localhost',
}

# FLASK_SECRET_KEY should be set to a secure random string with an appropriate
# length; 50 is reasonable. For the random generation to be secure, use
# ``random.SystemRandom()``
FLASK_SECRET_KEY = 'eCKJOOw3uQBR5pVDz3WIvYk3RsjORYoPRdzSUNJIeUEkm1Uvtq'

HMAC_ENCRYPTION_KEY = os.environ.get('CDIS_HMAC_ENCRYPTION_KEY', '')
OAUTH2 = {
    "client_id": os.environ.get('CDIS_PEREGRINE_CLIENT_ID'),
    "client_secret": os.environ.get("CDIS_PEREGRINE_CLIENT_SECRET"),
    "oauth_provider": os.environ.get("CDIS_USER_API_OAUTH", 'http://localhost:8000/oauth2/'),
    "redirect_uri": os.environ.get("CDIS_PEREGRINE_OAUTH_REDIRECT", 'localhost:5000/v0/oauth2/authorize'),
}

USER_API = "http://localhost:8000/"
SESSION_COOKIE_NAME = 'peregrine_session'

# verify project existence in dbgap or not
VERIFY_PROJECT = False
