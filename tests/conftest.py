import flask

import pytest
import requests

# Python 2 and 3 compatible
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch

from sheepdog.errors import AuthZError
from sheepdog.test_settings import JWT_KEYPAIR_FILES

from tests import utils


SUBMITTER_USERNAME = "submitter"
ADMIN_USERNAME = "admin"


@pytest.fixture(scope="session")
def iss():
    """
    ``iss`` field for tokens
    """
    return "localhost"


@pytest.fixture(scope="session")
def encoded_jwt(iss):
    def encoded_jwt_function(private_key, user):
        """
        Return an example JWT containing the claims and encoded with the private
        key.

        Args:
            private_key (str): private key
            user (generic User object): user object

        Return:
            str: JWT containing claims encoded with private key
        """
        kid = JWT_KEYPAIR_FILES.keys()[0]
        scopes = ["openid"]
        token = utils.generate_signed_access_token(
            kid, private_key, user, 3600, scopes, iss=iss, forced_exp_time=None
        )
        return token.token

    return encoded_jwt_function


@pytest.fixture(scope="session")
def create_user_header(encoded_jwt):
    def create_user_header_function(username, **kwargs):
        private_key = utils.read_file(
            "./integration/resources/keys/test_private_key.pem"
        )
        # set up a fake User object which has all the attributes needed
        # to generate a token
        user_properties = {
            "id": 1,
            "username": "submitter",
            "is_admin": False,
            "policies": [],
            "google_proxy_group_id": None,
        }
        user_properties.update(**kwargs)
        user = type("User", (object,), user_properties)
        token = encoded_jwt(private_key, user)
        return {"Authorization": "bearer " + token}

    return create_user_header_function


@pytest.fixture()
def submitter(create_user_header):
    return create_user_header(SUBMITTER_USERNAME)


@pytest.fixture()
def submitter_name():
    return SUBMITTER_USERNAME


@pytest.fixture()
def admin(create_user_header):
    return create_user_header(ADMIN_USERNAME, is_admin=True)


@pytest.yield_fixture
def client(app):
    """
    Overriding the `client` fixture from pytest_flask to fix this bug:
    https://github.com/pytest-dev/pytest-flask/issues/42
    Fixed in Flask 1.1.0
    """
    with app.test_client() as client:
        yield client

    while True:
        top = flask._request_ctx_stack.top
        if top is not None and top.preserved:
            top.pop()
        else:
            break


@pytest.fixture(scope="function")
def mock_arborist_requests(request):
    """
    This fixture returns a function which you call to mock the call to
    arborist client's auth_request method.
    By default, it returns a 200 response. If parameter "authorized" is set
    to False, it raises a 401 error.
    """

    def do_patch(authorized=True):
        def make_mock_response(*args, **kwargs):
            if not authorized:
                raise AuthZError('Mocked Arborist says no')
            mocked_response = MagicMock(requests.Response)
            mocked_response.status_code = 200

            def mocked_get(*args, **kwargs):
                return None
            mocked_response.get = mocked_get

            return mocked_response

        mocked_auth_request = MagicMock(side_effect=make_mock_response)

        patch_auth_request = patch("gen3authz.client.arborist.client.ArboristClient.auth_request", mocked_auth_request)
        patch_create_resource = patch("gen3authz.client.arborist.client.ArboristClient.create_resource", mocked_auth_request)

        patch_auth_request.start()
        patch_create_resource.start()

        request.addfinalizer(patch_auth_request.stop)
        request.addfinalizer(patch_create_resource.stop)

    return do_patch


@pytest.fixture(autouse=True)
def arborist_authorized(mock_arborist_requests):
    """
    By default, mocked arborist calls return Authorized.
    To mock an unauthorized response, use fixture
    "mock_arborist_requests(authorized=False)" in the test itself
    """
    mock_arborist_requests()
