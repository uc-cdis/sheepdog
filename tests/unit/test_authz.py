import pytest
from unittest.mock import patch, MagicMock
import time
import flask
import json
import base64
from sheepdog.auth import check_if_jwt_close_to_expiry
from sheepdog.auth import authorize, AUTHZ_CACHE, CACHE_SECONDS


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


def encode_jwt(payload):
    """Helper function to encode a JWT without a signature."""
    header = {"alg": "none", "typ": "JWT"}
    encoded_header = (
        base64.urlsafe_b64encode(json.dumps(header).encode()).rstrip(b"=").decode()
    )
    encoded_payload = (
        base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    )
    return f"{encoded_header}.{encoded_payload}."


def test_check_if_jwt_close_to_expiry():
    """Tests JWT expiration logic."""
    expired_token = encode_jwt({"data": "test", "exp": time.time() - 1000})
    # Token expires before cache expiration
    token_about_to_expire = encode_jwt(
        {"data": "test", "exp": time.time() + CACHE_SECONDS / 2}
    )
    valid_token = encode_jwt({"data": "test", "exp": time.time() + 1000})

    assert check_if_jwt_close_to_expiry(expired_token) is True
    assert check_if_jwt_close_to_expiry(token_about_to_expire) is True
    assert check_if_jwt_close_to_expiry(valid_token) is False


@patch("sheepdog.auth.check_if_jwt_close_to_expiry", return_value="jwt")
@patch("sheepdog.auth.check_if_jwt_expired", return_value=False)
def test_authorize_caching(
    mock_jwt, mock_jwt_expired, mock_flask_app, mock_auth_request
):
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


@patch("sheepdog.auth.check_if_jwt_close_to_expiry", return_value="jwt")
@patch("sheepdog.auth.check_if_jwt_expired", return_value=False)
def test_authorize_cache_invalidation(
    mock_jwt, mock_jwt_expired, mock_flask_app, mock_auth_request
):
    """Ensures cache is invalidated after a timeout period."""

    AUTHZ_CACHE.clear()

    with patch.object(flask, "current_app") as mock_app:
        mock_app.auth.auth_request = mock_auth_request

        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()
        mock_auth_request.reset_mock()

        time.sleep(1)  # Simulating cache expiration

        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()


@patch("sheepdog.auth.check_if_jwt_expired", return_value=False)
@patch("sheepdog.auth.check_if_jwt_close_to_expiry")
def test_authorize_caching_per_user(
    mock_jwt, mock_jwt_expired, mock_flask_app, mock_auth_request
):
    """
    Ensures `auth_request` is called separately for each unique user.
    Each user has a unique JWT, ensuring independent authorization checks.
    """

    AUTHZ_CACHE.clear()

    with patch.object(flask, "current_app") as mock_app:
        mock_app.auth.auth_request = mock_auth_request

        # Mock JWTs for two different users
        jwt_generator = iter(["jwt_for_user1", "jwt_for_user2"])

        # Configure `check_if_jwt_close_to_expiry` to return different JWTs per call
        mock_jwt.side_effect = lambda: next(jwt_generator)

        # First user makes a request (jwt_for_user1)
        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()
        mock_auth_request.reset_mock()

        # Second user makes a request (jwt_for_user2)
        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()


@patch("sheepdog.auth.check_if_jwt_expired", return_value=True)
@patch("sheepdog.auth.check_if_jwt_close_to_expiry", return_value="jwt")
def test_authorize_expired_token(
    mock_jwt, mock_jwt_expired, mock_flask_app, mock_auth_request
):
    """Ensures data is not pulled from cache when jwt is expired"""

    AUTHZ_CACHE.clear()

    with patch.object(flask, "current_app") as mock_app:
        mock_app.auth.auth_request = mock_auth_request

        authorize("program", "project", ["role1"])
        mock_auth_request.assert_called_once()

        authorize("program", "project", ["role1"])
        mock_auth_request.call_count == 2
