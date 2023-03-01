import pytest

from tests.integration.utils import put_cgci_blgsp


@pytest.fixture
def require_index_exists_on(app, monkeypatch):
    monkeypatch.setitem(app.config, "REQUIRE_FILE_INDEX_EXISTS", True)


@pytest.fixture
def require_index_exists_off(app, monkeypatch):
    monkeypatch.setitem(app.config, "REQUIRE_FILE_INDEX_EXISTS", False)


@pytest.fixture()
def use_ssl(request):
    try:
        # one of [False, True, None]
        return request.param
    except Exception:
        return None


@pytest.fixture()
def isolation_level(request):
    try:
        # one of ["READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE", None]
        return request.param
    except Exception:
        return None


@pytest.fixture()
def cgci_blgsp(client, submitter):
    put_cgci_blgsp(client, submitter)
