"""
This module will depend on the downstream authutils dependency.

eg:
``pip install git+https://git@github.com/NCI-GDC/authutils.git@1.2.3#egg=authutils``
or
``pip install git+https://git@github.com/uc-cdis/authutils.git@1.2.3#egg=authutils``
"""

import functools

from authutils.user import current_user
from authutils.token.validate import current_token
import flask
import re

from sheepdog.errors import AuthError, AuthZError


def authorize_for_project(*required_roles):
    """
    Wrap a function to allow access to the handler iff the user has at least
    one of the roles requested on the given project.
    """

    def wrapper(func):
        @functools.wraps(func)
        def authorize_and_call(program, project, *args, **kwargs):
            resource = "/programs/{}/projects/{}".format(program, project)
            try:
                auth_header = flask.request.headers["Authorization"]
                if auth_header:
                    items = auth_header.split(" ")
                    if len(items) == 2 and items[0].lower() == "bearer":
                        jwt = items[1]
                assert jwt
            except Exception:  # this is the MVP, okay?
                raise AuthError("didn't receive JWT correctly")
            authz = flask.current_app.auth.auth_request(
                jwt, "sheepdog", required_roles, [resource]
            )
            if not authz:
                raise AuthZError("user is unauthorized")
            return func(program, project, *args, **kwargs)

        return authorize_and_call

    return wrapper


def authorize(program, project, roles):
    resource = "/programs/{}/projects/{}".format(program, project)
    try:
        jwt = flask.request.headers["Authorization"].split("Bearer ")[1]
    except Exception:  # this is the MVP, okay?
        raise AuthError("didn't receive JWT correctly")
    authz = flask.current_app.auth.auth_request(
        jwt, "sheepdog", roles, [resource]
    )
    if not authz:
        raise AuthZError("user is unauthorized")
