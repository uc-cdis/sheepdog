import os
import json
from multiprocessing import Process

import pytest
import requests
import requests_mock
from mock import patch
from cdisutilstest.code.conftest import (
    indexd_client,
    indexd_server,
)
from flask.testing import make_test_environ_builder
from fence.jwt.token import generate_signed_access_token
from psqlgraph import PsqlGraphDriver
from gdcdatamodel.models import Edge, Node
from userdatamodel import models as usermd
from userdatamodel import Base as usermd_base
from userdatamodel.driver import SQLAlchemyDriver
from cdispyutils.hmac4 import get_auth
from dictionaryutils import DataDictionary, dictionary
from datamodelutils import models, validators

import sheepdog
import utils
from sheepdog.auth import roles
from sheepdog.test_settings import (
    PSQL_USER_DB_CONNECTION,
    Fernet,
    HMAC_ENCRYPTION_KEY,
    JWT_KEYPAIR_FILES,
    INDEXD,
)
from tests.api import app as _app, app_init
from tests.submission.test_endpoints import put_cgci_blgsp

try:
    reload  # Python 2.7
except NameError:
    try:
        from importlib import reload  # Python 3.4+
    except ImportError:
        from imp import reload # Python 3.0 - 3.3


def get_parent(path):
    print(path)
    return path[0:path.rfind('/')]


PATH_TO_SCHEMA_DIR = get_parent(os.path.abspath(os.path.join(os.path.realpath(__file__), os.pardir))) + '/tests/schemas'


@pytest.fixture(scope='session')
def pg_config():
    test_host = 'localhost'
    test_user = 'test'
    test_pass = 'test'
    test_db = 'sheepdog_automated_test'
    return dict(
        host=test_host,
        user=test_user,
        password=test_pass,
        database=test_db,
    )

@pytest.fixture
def app(tmpdir, request):

    gencode_json = tmpdir.mkdir("slicing").join("test_gencode.json")
    gencode_json.write(json.dumps({
        'a_gene': ['chr1', None, 200],
        'b_gene': ['chr1', 150,  300],
        'c_gene': ['chr1', 200,  None],
        'd_gene': ['chr1', None, None],
    }))

    _app.config.from_object("sheepdog.test_settings")

    app_init(_app)
    dictionary_setup(_app)

    _app.logger.setLevel(os.environ.get("GDC_LOG_LEVEL", "WARNING"))

    _app.jwt_public_keys = {_app.config['USER_API']: {
            'key-test': utils.read_file('resources/keys/test_public_key.pem')
    }}
    return _app


@pytest.fixture
def pg_driver(request, client):
    pg_driver = PsqlGraphDriver(**pg_config())

    def tearDown():
        with pg_driver.engine.begin() as conn:
            for table in Node().get_subclass_table_names():
                if table != Node.__tablename__:
                    conn.execute('delete from {}'.format(table))
            for table in Edge().get_subclass_table_names():
                if table != Edge.__tablename__:
                    conn.execute('delete from {}'.format(table))
            conn.execute('delete from versioned_nodes')
            conn.execute('delete from _voided_nodes')
            conn.execute('delete from _voided_edges')
            conn.execute('delete from transaction_snapshots')
            conn.execute('delete from transaction_documents')
            conn.execute('delete from transaction_logs')
            user_teardown()

    tearDown()
    user_setup()
    request.addfinalizer(tearDown)
    return pg_driver


def user_setup():
    key = Fernet(HMAC_ENCRYPTION_KEY)
    user_driver = SQLAlchemyDriver(PSQL_USER_DB_CONNECTION)
    with user_driver.session as s:
        for username in [
                'admin', 'unauthorized', 'submitter', 'member', 'test']:
            user = usermd.User(username=username, is_admin=False)
            keypair = usermd.HMACKeyPair(
                access_key=username + 'accesskey',
                secret_key=key.encrypt(username),
                expire=1000000,
                user=user)
            s.add(user)
            s.add(keypair)
        users = s.query(usermd.User).all()
        test_user = s.query(usermd.User).filter(
            usermd.User.username == 'test').first()
        test_user.is_admin = True
        projects = ['phs000218', 'phs000235', 'phs000178']
        admin = s.query(usermd.User).filter(
            usermd.User.username == 'admin').first()
        admin.is_admin = True
        user = s.query(usermd.User).filter(
            usermd.User.username == 'submitter').first()
        member = s.query(usermd.User).filter(
            usermd.User.username == 'member').first()
        for phsid in projects:
            p = usermd.Project(
                name=phsid, auth_id=phsid)
            usermd.AccessPrivilege(
                user=user, project=p, privilege=roles.values())
            usermd.AccessPrivilege(
                user=member, project=p, privilege=['_member_'])
            usermd.AccessPrivilege(
                user=admin, project=p, privilege=roles.values())

    return user_driver


def user_teardown():
    user_driver = SQLAlchemyDriver(PSQL_USER_DB_CONNECTION)
    with user_driver.session as session:
        meta = usermd_base.metadata
        for table in reversed(meta.sorted_tables):
            session.execute(table.delete())


def encoded_jwt(private_key, user):
    """
    Return an example JWT containing the claims and encoded with the private
    key.

    Args:
        private_key (str): private key
        user (userdatamodel.models.User): user object

    Return:
        str: JWT containing claims encoded with private key
    """
    kid = JWT_KEYPAIR_FILES.keys()[0]
    scopes = ['openid']
    return generate_signed_access_token(
        kid, private_key, user, 3600, scopes, forced_exp_time=None)


def create_user_header(pg_driver, username):
    private_key = utils.read_file('resources/keys/test_private_key.pem')

    user_driver = SQLAlchemyDriver(PSQL_USER_DB_CONNECTION)
    with user_driver.session as s:
        user = s.query(usermd.User).filter_by(username=username).first()
        token = encoded_jwt(private_key, user)
        return {'Authorization': 'bearer ' + token}


@pytest.fixture()
def submitter(pg_driver):
    return create_user_header(pg_driver, 'submitter')


@pytest.fixture()
def admin(pg_driver):
    return create_user_header(pg_driver, 'admin')


@pytest.fixture()
def member(pg_driver):
    return create_user_header(pg_driver, 'member')


@pytest.fixture()
def cgci_blgsp(client, admin):
    put_cgci_blgsp(client, admin)


def dictionary_setup(_app):
    url = 's3://testurl'
    session = requests.Session()
    adapter = requests_mock.Adapter()
    session.mount('s3', adapter)
    json_dict = json.load(open(PATH_TO_SCHEMA_DIR + '/dictionary.json'))
    adapter.register_uri('GET', url, json=json_dict, status_code=200)
    resp = session.get(url)

    with patch('requests.get') as get_mocked:
        get_mocked.return_value = resp
        datadictionary = DataDictionary(url=url)
        dictionary.init(datadictionary)
        from gdcdatamodel import models as md
        from gdcdatamodel import validators as vd
        models.init(md)
        validators.init(vd)
        sheepdog_blueprint = sheepdog.create_blueprint(
            'submission'
        )

        try:
            _app.register_blueprint(sheepdog_blueprint, url_prefix='/v0/submission')
        except AssertionError:
            _app.logger.info('Blueprint is already registered!!!')
