from .config import LEGACY_MODE

SIGNPOST = {
    "host": "http://localhost:8000/", 'version': 'v0',
    "auth": None}
AUTH = 'https://fake_auth_url'
INTERNAL_AUTH = 'https://fake_auth_url'
AUTH_ADMIN_CREDS = {
    'domain_name': 'some_domain',
    'username': 'iama_username',
    'password': 'iama_password',
    'auth_url': 'https://fake_auth_url',
    'user_domain_name': 'some_domain',
}
SUBMISSION = {
    "bucket": 'test_submission',
    "host": 'host',
}
STORAGE = {"s3": {"keys": {}, "kwargs": {}}}
STORAGE["s3"]["keys"]["host"] = {"access_key": "fake",
                                 "secret_key": "sooper_sekrit"}
STORAGE["s3"]["kwargs"]["host"] = {}

PSQLGRAPH = {
    'host': "localhost",
    'user': "test",
    'password': "test",
    'database': "automated_test",
}

GDC_PORTAL_ENDPOINT = 'http://fake_portal_endpoint_for_tests'
GDC_API_HOST = "localhost"
GDC_API_PORT = "443"

GDC_ES_INDEX = "gdc_legacy_test" if LEGACY_MODE else "gdc_test"
GDC_ES_HOST = "localhost"
GDC_ES_CONF = {"port": 9200}
GDC_ES_STATS_INDEX = "gdc_stats_test"

GEO_API = 'http://fake_geolocation_service'

# Slicing settings
SLICING = {
    'host': 'localhost',
    'gencode': 'REPLACEME',
}
PSQL_USER_DB_NAME = 'test_userapi'
PSQL_USER_DB_USERNAME = 'postgres'
PSQL_USER_DB_PASSWORD = 'postgres'
PSQL_USER_DB_HOST = 'localhost'

PSQL_USER_DB_CONNECTION = "postgresql://{name}:{password}@{host}/{db}".format(
    name=PSQL_USER_DB_USERNAME, password=PSQL_USER_DB_PASSWORD, host=PSQL_USER_DB_HOST, db=PSQL_USER_DB_NAME
)

FLASK_SECRET_KEY = 'flask_test_key'

from cryptography.fernet import Fernet

HMAC_ENCRYPTION_KEY = Fernet.generate_key()
OAUTH2 = {
    "client_id": "",
    "client_secret": "",
    "oauth_provider": "",
    "redirect_uri": "",
}

USER_API = "localhost"

VERIFY_PROJECT = False
AUTH_SUBMISSION_LIST = False
