import json
from mock import patch, MagicMock
import pytest
import requests
import uuid

from sheepdog.errors import AuthZError
from sheepdog.test_settings import JWT_KEYPAIR_FILES
from tests import utils


SUBMITTER_USERNAME = "submitter"


@pytest.fixture(scope="session")
def iss():
    """
    ``iss`` field for tokens
    """
    return "localhost"


@pytest.fixture(scope="session")
def encoded_jwt(iss):
    def encoded_jwt_function(private_key, user=None, client_id=None):
        """
        Return an example JWT containing the claims and encoded with the private
        key.

        Args:
            private_key (str): private key
            user (generic User object): user object

        Return:
            str: JWT containing claims encoded with private key
        """
        kid = list(JWT_KEYPAIR_FILES.keys())[0]
        scopes = ["openid"]
        token = utils.generate_signed_access_token(
            kid,
            private_key,
            user,
            3600,
            scopes,
            iss=iss,
            forced_exp_time=None,
            client_id=client_id,
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
            "username": username,
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


@pytest.fixture(params=["user", "client"])
def submitter_and_client_submitter(request, create_user_header, encoded_jwt):
    """
    Used to test select functionality with both a regular user token, and a token issued from
    the `client_credentials` flow, linked to a client and not to a user.
    """
    if request.param == "user":
        return create_user_header(SUBMITTER_USERNAME)
    else:
        private_key = utils.read_file(
            "./integration/resources/keys/test_private_key.pem"
        )
        token = encoded_jwt(private_key, client_id="test_client_id")
        return {"Authorization": "bearer " + token}


@pytest.fixture()
def submitter_name():
    return SUBMITTER_USERNAME


@pytest.fixture(scope="function")
def mock_arborist_requests(request):
    """
    This fixture returns a function which you call to mock the call to
    arborist client's auth_request method.
    By default, it returns a 200 response. If parameter "authorized" is set
    to False, it raises a 401 error.
    """

    def do_patch(authorized=True):
        def mock_auth_request_response(*args, **kwargs):
            if not authorized:
                raise AuthZError("Mocked Arborist says no")
            return True

        mocked_auth_request = MagicMock(side_effect=mock_auth_request_response)

        def make_mock_response(*args, **kwargs):
            if not authorized:
                raise AuthZError("Mocked Arborist says no")
            mocked_response = MagicMock(requests.Response)
            mocked_response.status_code = 200

            mocked_response.get = lambda *args, **kwargs: None

            return mocked_response

        mocked_create_resource = MagicMock(side_effect=make_mock_response)

        patch_auth_request = patch(
            "gen3authz.client.arborist.client.ArboristClient.auth_request",
            mocked_auth_request,
        )
        patch_create_resource = patch(
            "gen3authz.client.arborist.client.ArboristClient.create_resource",
            mocked_create_resource,
        )

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


@pytest.fixture(scope="function", autouse=True)
def mock_indexd_requests(request):
    """
    This fixture mocks calls made by indexclient to Indexd
    """
    _records = {}  # {did: record} all records currently in the mocked indexd DB

    def make_mock_response(method, url, *args, **kwargs):
        print(f"DEBUG: indexd request: {method} {url} {args} {kwargs}")
        print(f"DEBUG: indexd records: {list(_records.keys())}")
        resp = MagicMock
        resp.status_code = 200
        resp_data = None

        url = url.rstrip("/")
        if method == "GET":
            if url.endswith("/index"):  # "list records" endpoint
                resp_data = {"records": _records}
            else:  # "get record" endpoint
                did = url.split("/index/")[-1]
                if did in _records:
                    resp_data = _records[did]
                else:
                    resp.status_code = 404
                    raise requests.HTTPError(response=resp)
        elif method == "POST":
            body = json.loads(args[1]["data"])
            if "rev" not in body:
                body["rev"] = str(uuid.uuid4())[:6]
            if "did" not in body:
                body["did"] = str(uuid.uuid4())
            _records[body["did"]] = body
            resp_data = body
        elif method == "PUT":
            did = url.split("/index/")[-1]
            record = _records[did]
            body = json.loads(args[1]["data"])
            record.update(body)
            _records[record["did"]] = record
            resp_data = record

        resp.json = lambda: resp_data
        return resp

    mocked_requests = MagicMock
    mocked_requests.get = lambda url, *args, **kwargs: make_mock_response(
        "GET", url, args, kwargs
    )
    mocked_requests.post = lambda url, *args, **kwargs: make_mock_response(
        "POST", url, args, kwargs
    )
    mocked_requests.put = lambda url, *args, **kwargs: make_mock_response(
        "PUT", url, args, kwargs
    )
    mocked_requests.exceptions = requests.exceptions
    mocked_requests.HTTPError = requests.HTTPError
    requests_patch = patch(
        "indexclient.client.requests",
        mocked_requests,
    )
    requests_patch.start()
    request.addfinalizer(requests_patch.stop)
