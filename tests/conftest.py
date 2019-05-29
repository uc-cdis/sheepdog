import flask

import pytest

from sheepdog.auth import ROLES
from sheepdog.test_settings import JWT_KEYPAIR_FILES

from tests import utils


SUBMITTER_USERNAME = "submitter"
ADMIN_USERNAME = "admin"
MEMBER_USERNAME = "member"


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
            user (fake userdatamodel.models.User): user object

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
    def create_user_header_function(username, project_access, **kwargs):
        private_key = utils.read_file(
            "./integration/resources/keys/test_private_key.pem"
        )
        # set up a fake User object which has all the attributes that fence needs
        # to generate a token
        user_properties = {
            "id": 1,
            "username": "submitter",
            "is_admin": False,
            "project_access": project_access,
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
    project_ids = ["phs000218", "phs000235", "phs000178"]
    project_access = {project: ROLES.values() for project in project_ids}
    return create_user_header(SUBMITTER_USERNAME, project_access)


@pytest.fixture()
def submitter_name():
    return SUBMITTER_USERNAME


@pytest.fixture()
def admin(create_user_header):
    project_ids = ["phs000218", "phs000235", "phs000178"]
    project_access = {project: ROLES.values() for project in project_ids}
    return create_user_header(ADMIN_USERNAME, project_access, is_admin=True)


@pytest.fixture()
def member(create_user_header):
    project_ids = ["phs000218", "phs000235", "phs000178"]
    project_access = {project: ["_member"] for project in project_ids}
    return create_user_header(MEMBER_USERNAME, project_access)


@pytest.yield_fixture
def client(app):
    """
    Overriding the `client` fixture from pytest_flask to fix this bug:
    https://github.com/pytest-dev/pytest-flask/issues/42
    """
    with app.test_client() as client:
        yield client

    while True:
        top = flask._request_ctx_stack.top
        if top is not None and top.preserved:
            top.pop()
        else:
            break
