from collections import OrderedDict
from .config import LEGACY_MODE

INDEXD = {
    "host": "http://localhost:8000/", 'version': 'v0',
    "auth": ('fake_user', 'fake_password')}
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

DICTIONARY_URL = 'https://testdictionary.json'

STORAGE = {"s3": {"keys": {}, "kwargs": {}}}
STORAGE["s3"]["keys"]["host"] = {"access_key": "fake",
                                 "secret_key": "sooper_sekrit"}
STORAGE["s3"]["kwargs"]["host"] = {}

PSQLGRAPH = {
    'host': "localhost",
    'user': "test",
    'password': "test",
    'database': "sheepdog_automated_test",
}

SHEEPDOG_HOST = "localhost"
SHEEPDOG_PORT = "443"

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
BASE_URL = "localhost"

VERIFY_PROJECT = False
AUTH_SUBMISSION_LIST = False

JWT_KEYPAIR_FILES = OrderedDict([
    (
        'key-test',
        ('resources/keys/test_public_key.pem', 'resources/keys/test_private_key.pem'),
    ),
    (
        'key-test-2',
        ('resources/keys/test_public_key_2.pem', 'resources/keys/test_private_key_2.pem'),
    ),
])
