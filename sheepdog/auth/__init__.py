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

from sheepdog.errors import AuthNError, AuthZError


def get_jwt_from_header():
    try:
        jwt = None
        auth_header = flask.request.headers["Authorization"]
        if auth_header:
            items = auth_header.split(" ")
            if len(items) == 2 and items[0].lower() == "bearer":
                jwt = items[1]
        assert jwt, "Unable to parse header"
    except Exception as e:  # this is the MVP, okay? TODO better exception handling
        print(e)
        raise AuthNError("Didn't receive JWT correctly")
    return jwt


def authorize_for_project(*required_roles):
    """
    Wrap a function to allow access to the handler iff the user has at least
    one of the roles requested on the given project.
    """

    def wrapper(func):
        @functools.wraps(func)
        def authorize_and_call(program, project, *args, **kwargs):
            resource = "/programs/{}/projects/{}".format(program, project)
            jwt = get_jwt_from_header()
            authz = flask.current_app.auth.auth_request(
                jwt=jwt,
                service="sheepdog",
                methods=required_roles,
                resources=[resource]
            )
            if not authz:
                raise AuthZError("user is unauthorized")
            return func(program, project, *args, **kwargs)

        return authorize_and_call

    return wrapper


def authorize(program, project, roles):
    resource = "/programs/{}/projects/{}".format(program, project)
    jwt = get_jwt_from_header()
    authz = flask.current_app.auth.auth_request(
        jwt=jwt,
        service="sheepdog",
        methods=roles,
        resources=[resource]
    )
    if not authz:
        raise AuthZError("user is unauthorized")
