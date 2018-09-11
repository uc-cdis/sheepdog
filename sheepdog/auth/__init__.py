"""
This module will depend on the downstream authutils dependency.

eg:
``pip install git+https://git@github.com/NCI-GDC/authutils.git@1.2.3#egg=authutils``
or
``pip install git+https://git@github.com/uc-cdis/authutils.git@1.2.3#egg=authutils``
"""

import functools

from cdislogging import get_logger
import flask

import authutils
from authutils import ROLES, dbgap
from authutils.user import AuthError, current_user, set_global_user
from cdiserrors import AuthZError

from sheepdog import models

LOGGER = get_logger('sheepdog_auth')


def _log_import_error(module_name):
    """
    Log which module cannot be imported.

    Just in case this currently short list grows, make it a function.
    """
    LOGGER.info('Unable to import %s, assuming it is not there', module_name)


# planx only modules (for now)

# Separate try blocks in case one gets brought into gdc authutils.
# This is done with try blocks because when sheepdog.api imports
# sheepdog.auth you can't use flask.current_app. It hasn't been
# instantiated yet (application out of context error)

try:
    from authutils.token.validate import validate_request
except ImportError:
    _log_import_error('validate_request')


def _role_error_msg(user_name, roles, project):
    role_names = [
        role if role != '_member_' else 'read (_member_)' for role in roles
    ]
    return (
        "User {} doesn't have {} access in {}".format(
            user_name, ' or '.join(role_names), project
        )
    )


def get_program_project_roles(program, project):
    """
    A lot of places (submission entities etc.) confuse the terminology and have
    a ``project_id`` attribute which is actually ``{program}-{project}``, so
    in those places call this function like

        get_program_project_roles(*project_id.split('-', 1))

    Args:
        program (str): program name (NOT id)
        project (str): project name (NOT id)

    Return:
        Set[str]: roles
    """
    if not hasattr(flask.g, 'sheepdog_roles'):
        flask.g.sheepdog_roles = dict()

    if not (program, project) in flask.g.sheepdog_roles:
        user_roles = set()
        with flask.current_app.db.session_scope():
            if program:
                program_node = (
                    flask.current_app.db
                    .nodes(models.Program)
                    .props(name=program)
                    .scalar()
                )
                if program_node:
                    program_id = program_node.dbgap_accession_number
                    roles = current_user.projects.get(program_id, set())
                    user_roles.update(set(roles))
            if project:
                project_node = (
                    flask.current_app.db
                    .nodes(models.Project)
                    .props(code=project)
                    .scalar()
                )
                if project_node:
                    project_id = project_node.dbgap_accession_number
                    roles = current_user.projects.get(project_id, set())
                    user_roles.update(set(roles))
        flask.g.sheepdog_roles[(program, project)] = user_roles

    return flask.g.sheepdog_roles[(program, project)]


def authorize_for_project(*required_roles):
    """
    Wrap a function to allow access to the handler iff the user has at least
    one of the roles requested on the given project.
    """

    def wrapper(func):

        @functools.wraps(func)
        def authorize_and_call(program, project, *args, **kwargs):
            user_roles = get_program_project_roles(program, project)
            if not user_roles & set(required_roles):
                raise AuthZError(_role_error_msg(
                    current_user.username, required_roles, project
                ))
            return func(program, project, *args, **kwargs)

        return authorize_and_call

    return wrapper
