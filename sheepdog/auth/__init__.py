"""
This module will depend on the downstream authutils dependency.

eg:
``pip install git+https://git@github.com/NCI-GDC/authutils.git@1.2.3#egg=authutils``
or
``pip install git+https://git@github.com/uc-cdis/authutils.git@1.2.3#egg=authutils``
"""

import functools

import flask

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
                jwt = flask.request.headers["Authorization"].split("Bearer ")[1]
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
