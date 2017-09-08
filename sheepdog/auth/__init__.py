"""
Provides functions for handling user authentication and authorization.
"""

import functools

import cdis_oauth2client
from cdis_oauth2client import OAuth2Error
from cdispyutils.hmac4 import verify_hmac
from cdispyutils.hmac4.hmac4_auth_utils import HMAC4Error
from cryptography.fernet import Fernet
import flask
from flask_sqlalchemy_session import current_session
from userdatamodel.user import AccessPrivilege, HMACKeyPair, User

from sheepdog.auth.auth_driver import AuthDriver
from sheepdog.auth.federated_user import FederatedUser
from sheepdog.errors import (
    AuthError,
)


def admin_auth():
    check_user_credential()
    if not flask.g.user.user.is_admin:
        raise AuthError("You don't have admin access to perform this action")


def authorize_for_project(*roles):
    """
    Wrap a flask blueprint route to do the following:

    #. Allow access to the handler iff the user has at least one of the roles
       requested on the given project
    #. Set the global Flask variable `g.user` to a `FederatedUser`.
    """
    def wrapper(func):
        @functools.wraps(func)
        def authorize_and_call(program, project, *args, **kwargs):
            project_id = '{}-{}'.format(program, project)
            check_user_credential()
            # Get intersection of user's roles and requested roles
            if not set(flask.g.user.roles[project_id]) & set(roles):
                raise AuthError(
                    role_error_msg(flask.g.user.username, roles, project_id)
                )
            return func(program, project, *args, **kwargs)
        return authorize_and_call
    return wrapper


def check_user_credential():
    try:
        username = cdis_oauth2client.get_username()
        get_user(username)
    except OAuth2Error as oauth2_error:
        try:
            verify_hmac(
                flask.request, 'submission', get_secret_key_and_user
            )
        except HMAC4Error as hmac_error:
            flask.current_app.logger.exception('Failed to verify OAuth')
            flask.current_app.logger.exception('Failed to verify hmac')
            raise AuthError(oauth2_error.message + '; ' + hmac_error.message)


def get_secret_key_and_user(access_key):
    hmac_keypair = (
        current_session.query(HMACKeyPair)
        .filter(HMACKeyPair.access_key == access_key)
        .first()
    )
    if not hmac_keypair:
        raise AuthError(
            "Access key doesn't exist, or the key in use does not match any"
            " existing entries"
        )
    if not hasattr(flask.g, 'user'):
        flask.g.user = FederatedUser(hmac_keypair=hmac_keypair)
    key = Fernet(bytes(flask.current_app.config['HMAC_ENCRYPTION_KEY']))
    return key.decrypt(bytes(hmac_keypair.secret_key))


def get_user(username):
    user = (
        current_session.query(User)
        .filter(User.username == username)
        .first()
    )
    if not user:
        raise AuthError("User doesn't exist.")
    flask.g.user = flask.g.get('user', FederatedUser(user=user))


def require_auth():
    """
    Check if a request is authenticated.
    """
    check_user_credential()
    if not flask.g.user:
        raise AuthError('This endpoint requires authentication')


def role_error_msg(user_name, roles, project):
    role_names = [
        role if role != '_member_' else 'read (_member_)' for role in roles
    ]
    return (
        "User {} doesn't have {} access in {}".format(
            user_name, ' or '.join(role_names), project
        )
    )


def set_global_user(func):
    """Wrap a Flask blueprint view function to set the global user."""
    @functools.wraps(func)
    def f(*args, **kwargs):
        check_user_credential()
        return func(*args, **kwargs)
    return f
