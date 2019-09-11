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
from cdislogging import get_logger
import flask

from sheepdog.errors import AuthNError, AuthZError


logger = get_logger(__name__)


def get_jwt_from_header():
    jwt = None
    auth_header = flask.request.headers.get("Authorization")
    if auth_header:
        items = auth_header.split(" ")
        if len(items) == 2 and items[0].lower() == "bearer":
            jwt = items[1]
    if not jwt:
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


def create_resource(program, project=None):
    resource = "/programs/{}".format(program)
    if project:
        resource += "/projects/{}".format(project)
    logger.info("Creating arborist resource {}".format(resource))

    json_data = {
        "name": resource,
        "description": "Created by sheepdog"  # TODO use authz provider field
    }
    resp = flask.current_app.auth.create_resource(
        parent_path="",
        resource_json=json_data,
        create_parents=True
    )
    if resp and resp.get("error"):
        logger.error("Unable to create resource: code {} - {}".format(resp.error.code, resp.error.message))
