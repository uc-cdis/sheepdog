import pytest
from unittest.mock import patch, MagicMock
from time import sleep
import flask
from sheepdog.auth import authorize, AUTHZ_CACHE


@pytest.fixture
def mock_flask_app():
    """Fixture to provide a test Flask app context."""
    app = flask.Flask(__name__)
    with app.app_context():
        yield app


@pytest.fixture
def mock_auth_request():
    """Fixture to mock the auth_request method."""
    return MagicMock(return_value=True)


@patch("sheepdog.auth.get_jwt_from_header", return_value="jwt")
def test_authorize_caching(mock_jwt, mock_flask_app, mock_auth_request):
    """
    Unit test for `authorize` function to verify caching behavior.
    Ensures that:
    - The first call with a specific set of parameters invokes `auth_request`.
    - Subsequent calls with the same parameters retrieve the response from cache.
    - A new set of parameters triggers a new `auth_request` call.
    """

    AUTHZ_CACHE.clear()

    with patch.object(flask, "current_app") as mock_app:
        mock_app.auth.auth_request = mock_auth_request

        # First call: auth_request should be invoked
        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()
        mock_auth_request.reset_mock()

        # Second call with the same params: should use cache, not call auth_request
        authorize("program", "project", ["role1"])
        mock_auth_request.assert_not_called()

        # New role: should trigger a new auth_request call
        authorize("program", "project", ["role2"])
        mock_auth_request.assert_called_once()


@patch("sheepdog.auth.get_jwt_from_header", return_value="jwt")
def test_authorize_cache_invalidation(mock_jwt, mock_flask_app, mock_auth_request):
    """Ensures cache is invalidated after a timeout period."""

    AUTHZ_CACHE.clear()

    with patch.object(flask, "current_app") as mock_app:
        mock_app.auth.auth_request = mock_auth_request

        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()
        mock_auth_request.reset_mock()

        sleep(1)  # Simulating cache expiration

        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()


@patch("sheepdog.auth.get_jwt_from_header")
def test_authorize_caching_per_user(mock_jwt, mock_flask_app, mock_auth_request):
    """
    Ensures `auth_request` is called separately for each unique user.
    Each user has a unique JWT, ensuring independent authorization checks.
    """

    AUTHZ_CACHE.clear()

    with patch.object(flask, "current_app") as mock_app:
        mock_app.auth.auth_request = mock_auth_request

        # Mock JWTs for two different users
        jwt_generator = iter(["jwt_for_user1", "jwt_for_user2"])

        # Configure `get_jwt_from_header` to return different JWTs per call
        mock_jwt.side_effect = lambda: next(jwt_generator)

        # First user makes a request (jwt_for_user1)
        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()
        mock_auth_request.reset_mock()

        # Second user makes a request (jwt_for_user2)
        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()
