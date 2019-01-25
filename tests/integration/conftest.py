# pylint: disable=unused-import
import hashlib
import os
import json
import random

import uuid

import pytest
import requests
import requests_mock
from mock import patch
from cdisutilstest.code.conftest import (
    indexd_client,
    indexd_server,
)

from fence.jwt.token import generate_signed_access_token
from psqlgraph import PsqlGraphDriver
from gdcdatamodel import models as m
from userdatamodel import models as usermd
from userdatamodel import Base as usermd_base
from userdatamodel.driver import SQLAlchemyDriver
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
)
from sheepdog import test_settings
from tests.integration.api import app as _app, app_init
from tests.integration.submission.test_upload import DEFAULT_URL

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


PATH_TO_SCHEMA_DIR = get_parent(os.path.abspath(os.path.join(os.path.realpath(__file__), os.pardir))) + '/integration/schemas'


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


@pytest.fixture(scope='function')
def client_toggled(config, client, request):
    """
    Will toggle app config parameters for the test using this and return test client

    USAGE:
    @pytest.mark.config_toggle(parameters={'PARAM1': VAL1, 'PARAM2': VAL2})
    def test_mytest(client_toggled):
        ...

    NOTE: App config gets reset each time function-scoped app fixture is executed,
    no need to reset here
    """
    # node.get_marker was deprecated https://github.com/pytest-dev/pytest/issues/4546
    params = request.node.get_closest_marker('config_toggle')

    for parameter, value in params.kwargs['parameters'].items():
        config[parameter] = value

    yield client

    # reset app config to the original state
    config.from_object('sheepdog.test_settings')

    for param in params.kwargs['parameters']:
        if not hasattr(test_settings, param):
            config.pop(param)


@pytest.fixture
def pg_driver(request, client, pg_config):
    pg_driver = PsqlGraphDriver(**pg_config)

    def tearDown():
        with pg_driver.engine.begin() as conn:
            for table in m.Node().get_subclass_table_names():
                if table != m.Node.__tablename__:
                    conn.execute('delete from {}'.format(table))
            for table in m.Edge().get_subclass_table_names():
                if table != m.Edge.__tablename__:
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
def put_program(client, admin):
    def put_program_helper(headers=admin, name='CGCI', phsid='phs000235', status_code=200):
        path = '/v0/submission'
        data = json.dumps({
            'type': 'program',
            'name': name,
            'dbgap_accession_number': phsid,
        })
        resp = client.put(path, headers=headers, data=data)
        assert resp.status_code == status_code, resp.data
        return resp

    return put_program_helper


@pytest.fixture()
def put_cgci_blgsp(client, admin):
    def put_cgci_blgsp_helper(
            headers=admin, code='BLGSP', phsid='phs000527',
            status_code=200, case_range=None, case_prefix=None):

        path = '/v0/submission/CGCI/'
        data = {
            "type": "project",
            "code": code,
            "dbgap_accession_number": phsid,
            "name": "Burkitt Lymphoma Genome Sequencing Project",
            "state": "open"
        }
        if case_range and case_prefix:
            data['bypass_case_range'] = case_range
            data['bypass_case_prefix'] = case_prefix

        data = json.dumps(data)
        resp = client.put(path, headers=headers, data=data)
        assert resp.status_code == status_code, resp.data
        return resp

    return put_cgci_blgsp_helper


@pytest.fixture()
def cgci_blgsp(put_program, put_cgci_blgsp):
    put_program()
    put_cgci_blgsp()


@pytest.fixture()
def put_tcga_brca(client, admin):
    def put_tcga_brca_helper(headers=admin):
        data = json.dumps({
            "type": "project",
            "code": "BRCA",
            "name": "TEST",
            "dbgap_accession_number": None,
            "state": "open"
        })
        resp = client.put('/v0/submission/TCGA/', headers=headers, data=data)
        assert resp.status_code == 200, resp.data
        return resp

    return put_tcga_brca_helper


@pytest.fixture()
def tcga_brca(put_program, put_tcga_brca):
    put_program(name='TCGA', phsid='phs000178')
    put_tcga_brca()


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


@pytest.fixture
def data_release(pg_driver):
    """
    Args:
        pg_driver (psqlgraph.PsqlGraphDriver):
    """
    releases = []
    with pg_driver.session_scope() as sxn:
        release = m.DataRelease(node_id=str(uuid.uuid4()))
        release.major_version = 10
        release.minor_version = 2
        release.released = True
        release.release_data = '2018-09-27'

        sxn.add(release)
        releases.append(release)
        r2 = m.DataRelease(node_id=str(uuid.uuid4()))
        r2.major_version = 11
        r2.minor_version = 0
        r2.released = False
        sxn.add(r2)
        releases.append(r2)

    # return current release number same as the data_release with released==False
    yield "11.0"
    with pg_driver.session_scope() as sxn:
        for release in releases:
            sxn.delete(release)


@pytest.fixture
def multiple_data_release(pg_driver):
    """
    Args:
        pg_driver (psqlgraph.PsqlGraphDriver):
    """
    releases = []
    with pg_driver.session_scope() as sxn:
        release = m.DataRelease(node_id=str(uuid.uuid4()))
        release.major_version = 10
        release.minor_version = 2
        release.released = False
        release.release_data = '2018-09-27'

        sxn.add(release)
        releases.append(release)
        r2 = m.DataRelease(node_id=str(uuid.uuid4()))
        r2.major_version = 11
        r2.minor_version = 0
        r2.released = False
        sxn.add(r2)
        releases.append(r2)

    # return current release number same as the data_release with released==False
    yield "11.0"
    with pg_driver.session_scope() as sxn:
        for release in releases:
            sxn.delete(release)


@pytest.fixture
def released_file(pg_driver, indexd_client, data_release):
    """
    Args:
        pg_driver (psqlgraph.PsqlGraphDriver):
    """
    doc = create_random_index(indexd_client, release=data_release)

    # create node
    with pg_driver.session_scope() as sxn:
        exp = m.ExperimentalMetadata(node_id=doc.did)
        exp.submitter_id = "0"
        exp.state = "released"
        exp.project_id = 'CGCI-BLGSP'
        exp.file_state = "validated"
        pg_driver.node_insert(exp)
    yield doc
    doc = indexd_client.get(doc.did)
    doc.delete()

    with pg_driver.session_scope() as sxn:
        sxn.delete(exp)


@pytest.fixture
def unreleased_file(pg_driver, indexd_client):
    doc = create_random_index(indexd_client, "10.2")

    # create node
    with pg_driver.session_scope() as sxn:
        exp = m.ExperimentalMetadata(node_id=doc.did)
        exp.submitter_id = "0"
        exp.state = "released"
        exp.project_id = 'CGCI-BLGSP'
        exp.file_state = "validated"
        pg_driver.node_insert(exp)
    yield doc
    doc.delete()

    with pg_driver.session_scope() as sxn:
        sxn.delete(exp)


def create_random_index(index_client, release):
    """
    Shorthand for creating new index entries for test purposes.
    Note:
        Expects index client v1.5.2 and above
    Args:
        index_client (indexclient.client.IndexClient): pytest fixture for index_client
        passed from actual test functions
        release (str): release number
    Returns:
        indexclient.client.Document: the document just created
    """

    did = str(uuid.uuid4())

    md5_hasher = hashlib.md5()
    md5_hasher.update(did.encode("utf-8"))
    hashes = {'md5': md5_hasher.hexdigest()}

    metadata = {"release_number": release} if release else {}
    doc = index_client.create(
        did=did,
        hashes=hashes,
        size=random.randint(10, 1000),
        acl=["a", "b"],
        version="1",
        metadata=metadata,
        file_name="{}_warning_huge_file.svs".format(did),
        urls=[DEFAULT_URL],
        urls_metadata={DEFAULT_URL: {"state": "validated", "type": "cleversafe"}}
    )

    return doc
