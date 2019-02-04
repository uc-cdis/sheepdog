from imp import reload
import os
import json
from multiprocessing import Process

from indexd import default_settings, get_app as get_indexd_app
from indexclient.client import IndexClient
import pytest
import requests
import requests_mock
from mock import patch
from flask.testing import make_test_environ_builder
from psqlgraph import PsqlGraphDriver
from datamodelutils import models
from cdispyutils.hmac4 import get_auth
from dictionaryutils import DataDictionary, dictionary
from datamodelutils import models, validators

import sheepdog

from sheepdog.test_settings import SIGNPOST
from tests.integration.datadictwithobjid.api import app as _app, app_init, indexd_init
from tests.integration.datadictwithobjid.submission.test_endpoints import put_cgci_blgsp
from tests import utils


def get_parent(path):
    print(path)
    return path[0 : path.rfind("/")]


PATH_TO_SCHEMA_DIR = (
    get_parent(os.path.abspath(os.path.join(os.path.realpath(__file__), os.pardir)))
    + "/datadictwithobjid/schemas"
)


@pytest.fixture(scope="session")
def pg_config():
    test_host = "localhost"
    test_user = "test"
    test_pass = "test"
    test_db = "sheepdog_automated_test"
    return dict(host=test_host, user=test_user, password=test_pass, database=test_db)


@pytest.fixture
def require_index_exists_on(app, monkeypatch):
    monkeypatch.setitem(app.config, "REQUIRE_FILE_INDEX_EXISTS", True)


@pytest.fixture
def require_index_exists_off(app, monkeypatch):
    monkeypatch.setitem(app.config, "REQUIRE_FILE_INDEX_EXISTS", False)


def wait_for_indexd_alive(port):
    url = "http://localhost:{}/_status".format(port)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return wait_for_indexd_alive(port)
    else:
        return


def wait_for_indexd_not_alive(port):
    url = "http://localhost:{}/_status".format(port)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return
    else:
        return wait_for_indexd_not_alive(port)


@pytest.fixture
def app(tmpdir, request):

    port = 8000
    dictionary_setup(_app)
    # this is to make sure sqlite is initialized
    # for every unit test
    reload(default_settings)

    # fresh files before running
    for filename in ["auth.sq3", "index.sq3", "alias.sq3"]:
        if os.path.exists(filename):
            os.remove(filename)
    indexd_app = get_indexd_app()

    indexd_init(*SIGNPOST["auth"])
    indexd = Process(target=indexd_app.run, args=["localhost", port])
    indexd.start()
    wait_for_indexd_alive(port)

    gencode_json = tmpdir.mkdir("slicing").join("test_gencode.json")
    gencode_json.write(
        json.dumps(
            {
                "a_gene": ["chr1", None, 200],
                "b_gene": ["chr1", 150, 300],
                "c_gene": ["chr1", 200, None],
                "d_gene": ["chr1", None, None],
            }
        )
    )

    def teardown():
        for filename in ["auth.sq3", "index.sq3", "alias.sq3"]:
            if os.path.exists(filename):
                os.remove(filename)

        indexd.terminate()
        wait_for_indexd_not_alive(port)

    _app.config.from_object("sheepdog.test_settings")
    _app.config["PATH_TO_SCHEMA_DIR"] = PATH_TO_SCHEMA_DIR

    request.addfinalizer(teardown)

    app_init(_app)

    _app.logger.setLevel(os.environ.get("GDC_LOG_LEVEL", "WARNING"))

    _app.jwt_public_keys = {
        _app.config["USER_API"]: {
            "key-test": utils.read_file(
                "./integration/resources/keys/test_public_key.pem"
            )
        }
    }
    return _app


@pytest.fixture
def pg_driver(request, client):
    pg_driver = PsqlGraphDriver(**pg_config())

    def tearDown():
        with pg_driver.engine.begin() as conn:
            for table in models.Node().get_subclass_table_names():
                if table != models.Node.__tablename__:
                    conn.execute("delete from {}".format(table))
            for table in models.Edge().get_subclass_table_names():
                if table != models.Edge.__tablename__:
                    conn.execute("delete from {}".format(table))
            conn.execute("delete from versioned_nodes")
            conn.execute("delete from _voided_nodes")
            conn.execute("delete from _voided_edges")
            conn.execute("delete from transaction_snapshots")
            conn.execute("delete from transaction_documents")
            conn.execute("delete from transaction_logs")

    tearDown()
    request.addfinalizer(tearDown)
    return pg_driver


@pytest.fixture()
def cgci_blgsp(client, admin):
    put_cgci_blgsp(client, admin)


@pytest.fixture()
def index_client():
    return IndexClient(SIGNPOST["host"], SIGNPOST["version"], SIGNPOST["auth"])


def dictionary_setup(_app):
    url = "s3://testurl"
    session = requests.Session()
    adapter = requests_mock.Adapter()
    session.mount("s3", adapter)
    json_dict = json.load(open(PATH_TO_SCHEMA_DIR + "/dictionary.json"))
    adapter.register_uri("GET", url, json=json_dict, status_code=200)
    resp = session.get(url)

    with patch("requests.get") as get_mocked:
        get_mocked.return_value = resp
        datadictionary = DataDictionary(url=url)
        dictionary.init(datadictionary)
        from gdcdatamodel import models as md
        from gdcdatamodel import validators as vd

        models.init(md)
        validators.init(vd)
