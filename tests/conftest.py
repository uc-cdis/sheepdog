
from cdispyutils.hmac4 import get_auth
from flask.testing import make_test_environ_builder
import sys
from sheepdog.auth import roles
from signpost import Signpost
from multiprocessing import Process
from gdcdatamodel.models import Edge, Node
from psqlgraph import PsqlGraphDriver
import pytest
from tests.api import app as _app, app_init
from mock import patch
import requests
from cdis_oauth2client import OAuth2Client
from elasticsearch import Elasticsearch
from sheepdog.test_settings import PSQL_USER_DB_CONNECTION
from sheepdog.test_settings import Fernet, HMAC_ENCRYPTION_KEY
from userdatamodel import models as usermd
from userdatamodel import Base as usermd_base
from userdatamodel.driver import SQLAlchemyDriver
from cdisutilstest.code.storage_client_mock import get_client
import os
import json
from dictionaryutils import DataDictionary
import gdcdatamodel

here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, here)

def get_parent(path):
    print(path)
    return path[0:path.rfind('/')]

class UserapiTestSettings(object):
    from boto.s3.connection import OrdinaryCallingFormat
    MOCK_AUTH = True
    MOCK_STORAGE = True
    SHIBBOLETH_HEADER = ''
    DB = 'postgresql://postgres@localhost:5432/test_userapi'
    STORAGE_CREDENTIALS = {
        "cleversafe": {
            'aws_access_key_id': '',
            'aws_secret_access_key': '',
            'host': 'somemanager.osdc.io',
            'public_host': 'someobjstore.datacommons.io',
            'port': 443,
            'is_secure': True,
            'username': 'someone',
            'password': 'somepass',
            "calling_format": OrdinaryCallingFormat(),
            "is_mocked": True
        }
    }
    CEPH = {
        'aws_access_key_id': '',
        'aws_secret_access_key': '',
        'host': '',
        'port': 443,
        'is_secure': True}
    AWS = {
        'aws_access_key_id': '',
        'aws_secret_access_key': '',
    }

    APPLICATION_ROOT = '/'
    DEBUG = True
    OAUTH2_PROVIDER_ERROR_URI = "/oauth2/errors"
    HOST_NAME = ''
    SHIBBOLETH_HEADER = 'persistent_id'
    SSO_URL = ''
    SINGLE_LOGOUT = ''

    LOGOUT = ""
    BIONIMBUS_ACCOUNT_ID = -1
    ENABLE_CSRF_PROTECTION = False


@pytest.fixture(scope='session')
def pg_config():
    test_host = 'localhost'
    test_user = 'test'
    test_pass = 'test'
    test_db = 'automated_test'
    return dict(
        host=test_host,
        user=test_user,
        password=test_pass,
        database=test_db,
    )


def wait_for_signpost_alive(port):
    url = 'http://localhost:{}'.format(port)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return wait_for_signpost_alive(port)
    else:
        return


def wait_for_signpost_not_alive(port):
    url = 'http://localhost:{}'.format(port)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return
    else:
        return wait_for_signpost_not_alive(port)


def run_signpost(port):
    Signpost({"driver": "inmemory", "layers": ["validator"]}).run(
        host="localhost", port=port, debug=False)


@pytest.fixture
def app(tmpdir, request):

    # import sheepdog
    # sheepdog_blueprint = sheepdog.blueprint.create_blueprint(
    #     gdcdictionary.gdcdictionary, gdcdatamodel.models
    # )

    port = 8000
    signpost = Process(target=run_signpost, args=[port])
    signpost.start()
    wait_for_signpost_alive(port)

    gencode_json = tmpdir.mkdir("slicing").join("test_gencode.json")
    gencode_json.write(json.dumps({
        'a_gene': ['chr1', None, 200],
        'b_gene': ['chr1', 150,  300],
        'c_gene': ['chr1', 200,  None],
        'd_gene': ['chr1', None, None],
    }))

    def teardown():
        signpost.terminate()
        wait_for_signpost_not_alive(port)

    _app.config.from_object("sheepdog.test_settings")
    _app.config['SLICING']['gencode'] = str(gencode_json.realpath())

    request.addfinalizer(teardown)

    app_init(_app)
    #_app.register_blueprint(sheepdog_blueprint, url_prefix='/v0/submission')

    _app.logger.setLevel(os.environ.get("GDC_LOG_LEVEL", "WARNING"))

    return _app


@pytest.fixture
def userapi_client(app, request):
    # fixture to setup userapi client
    patcher = patch(
        'userapi.resources.storage.get_client',
        get_client)
    patcher.start()
    userapi_app_init(userapi_app, UserapiTestSettings)
    userapi_app.test = True
    userapi_client_obj = userapi_app.test_client()
    oauth_client = utils.create_client(
        'internal_service',
        'https://localhost',
        app.config['PSQL_USER_DB_CONNECTION'],
        name='sheepdog', description='', auto_approve=True, is_admin=True
    )
    app.config['OAUTH2'] = {
        'client_id': oauth_client[0],
        'client_secret': oauth_client[1],
        'oauth_provider': '/oauth2/',
        'redirect_uri': 'https://localhost',
    }
    app.config['USER_API'] = '/'
    app.oauth2 = OAuth2Client(**app.config['OAUTH2'])

    def fin():
        utils.drop_client('sheepdog', app.config['PSQL_USER_DB_CONNECTION'])
    request.addfinalizer(fin)
    return userapi_client_obj


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
        print(users)
        test_user = s.query(usermd.User).filter(
            usermd.User.username == 'test').first()
        test_user.is_admin =True
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
            ua = usermd.AccessPrivilege(
                user=user, project=p, privilege=roles.values())
            s.add(ua)
            ua = usermd.AccessPrivilege(
                user=member, project=p, privilege=['_member_'])
            s.add(ua)

    return user_driver


def user_teardown():
    user_driver = SQLAlchemyDriver(PSQL_USER_DB_CONNECTION)
    with user_driver.session as session:
        meta = usermd_base.metadata
        for table in reversed(meta.sorted_tables):
            session.execute(table.delete())


@pytest.fixture()
def submitter(app, request):
    def build_header(path, method, role='submitter'):
        auth = get_auth(role + 'accesskey', role, 'submission')
        environ = make_test_environ_builder(app, path=path, method=method)
        request = environ.get_request()
        request.headers = dict(request.headers)
        auth.__call__(request)
        return request.headers
    return build_header


@pytest.fixture(scope='session')
def es_setup(request):
    es = Elasticsearch(["localhost"], port=9200)

    def es_teardown():
        es.indices.delete(
            index=INDEX,
            ignore=404,  # ignores error if index doesn't exists
        )
    request.addfinalizer(es_teardown)

    es.indices.create(
        index=INDEX,
        body=mappings.index_settings(),
        ignore=400,  # ignores error if index already exists
    )

    es.indices.put_mapping(index=INDEX, doc_type="file", body=mappings.get_file_es_mapping())
    es.indices.put_mapping(index=INDEX, doc_type="project", body=mappings.get_project_es_mapping())
    es.indices.put_mapping(index=INDEX, doc_type="case", body=mappings.get_case_es_mapping())
    es.indices.put_mapping(index=INDEX, doc_type="annotation", body=mappings.get_annotation_es_mapping())

    json_data = open(os.path.join(os.path.dirname(__file__), 'data/projects.json'))
    data = json.load(json_data)
    for p in data["data"]["hits"]:
        es.index(index=INDEX, doc_type=project_model.doc_type, body=p)

    json_data = open(os.path.join(os.path.dirname(__file__), 'data/files.json'))
    data = json.load(json_data)
    for f in data["data"]["hits"]:
        es.index(index=INDEX, doc_type=f_model.doc_type, body=f)

    json_data = open(os.path.join(os.path.dirname(__file__), 'data/cases.json'))
    data = json.load(json_data)
    for pa in data["data"]["hits"]:
        es.index(index=INDEX, doc_type=case_model.doc_type, body=pa)

    json_data = open(os.path.join(os.path.dirname(__file__), 'data/annotations.json'))
    data = json.load(json_data)
    for a in data["data"]["hits"]:
        es.index(index=INDEX, doc_type=annotation_model.doc_type, body=a)

    es.indices.refresh(index=INDEX)

    json_data.close()
