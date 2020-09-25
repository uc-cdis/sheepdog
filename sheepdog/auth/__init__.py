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
from sheepdog.globals import ROLES


logger = get_logger(__name__)

try:
    from authutils.token.validate import validate_request
except ImportError:
    logger.warning(
        "Unable to import authutils validate_request. Sheepdog will error if config AUTH_SUBMISSION_LIST is set to "
        "True (note that it is True by default) "
    )


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
                resources=[resource],
            )
            if not authz:
                raise AuthZError("user is unauthorized")
            return func(program, project, *args, **kwargs)

        return authorize_and_call

    return wrapper


def require_sheepdog_program_admin(func):
    """
    Wrap a function to allow access to the handler if the user has access to
    the resource /services/sheepdog/submission/program (Sheepdog program admin)
    """

    @functools.wraps(func)
    def authorize_and_call(*args, **kwargs):
        jwt = get_jwt_from_header()
        authz = flask.current_app.auth.auth_request(
            jwt=jwt,
            service="sheepdog",
            methods="*",
            resources=["/services/sheepdog/submission/program"],
        )
        if not authz:
            raise AuthZError("Unauthorized: User must be Sheepdog program admin")
        return func(*args, **kwargs)

    return authorize_and_call


def require_sheepdog_project_admin(func):
    """
    Wrap a function to allow access to the handler if the user has access to
    the resource /services/sheepdog/submission/project (Sheepdog project admin)
    """

    @functools.wraps(func)
    def authorize_and_call(*args, **kwargs):
        jwt = get_jwt_from_header()
        authz = flask.current_app.auth.auth_request(
            jwt=jwt,
            service="sheepdog",
            methods="*",
            resources=["/services/sheepdog/submission/project"],
        )
        if not authz:
            raise AuthZError("Unauthorized: User must be Sheepdog project admin")
        return func(*args, **kwargs)

    return authorize_and_call


def authorize(program, project, roles, resources_tmp=None):
    resource = "/programs/{}/projects/{}".format(program, project)

    resources = []
    if resources_tmp:
        for resource_tmp in resources_tmp:
            resources.append(resource + resource_tmp)
    else:
        resources = [resource]

    jwt = get_jwt_from_header()
    authz = flask.current_app.auth.auth_request(
        jwt=jwt, service="sheepdog", methods=roles, resources=resources
    )

    if not authz:
        raise AuthZError("user is unauthorized")


def create_resource(program, project=None, data=None):
    logger.warn("LUCA RESOURCE ATTENTION")
    logger.warn(data)   # {'type': 'subject', 'persons': [{'submitter_id': 'lavefrrg'}], 'submitter_id': 'test_sub_1', 'state': 'validated'}
    

    resource = "/programs/{}".format(program)

    if project:
        resource += "/projects/{}".format(project)

    stop_node = flask.current_app.node_authz_entity
    person_node = flask.current_app.subject_entity
    logger.warn(stop_node)
    logger.warn(person_node)
    logger.warn(stop_node[:-1])
    logger.warn(person_node[:-1])

    if data and data["type"] == "person":
        resource += "/persons/{}".format(data["submitter_id"])
    elif data and data["type"] == "subject":
        resource += "/persons/{}/subjects/{}".format(data["persons"][0]["submitter_id"], data["submitter_id"])
    logger.warn(resource)


    logger.info("Creating arborist resource {}".format(resource))

    json_data = {
        "name": resource,
        "description": "Created by sheepdog",  # TODO use authz provider field
    }
    resp = flask.current_app.auth.create_resource(
        parent_path="", resource_json=json_data, create_parents=True
    )
    if resp and resp.get("error"):
        logger.error(
            "Unable to create resource: code {} - {}".format(
                resp.error.code, resp.error.message
            )
        )


def check_resource_access(program, project, nodes):
    subject_submitter_ids = []
    stop_node = flask.current_app.node_authz_entity_name

    for node in nodes:
        if node.label == stop_node:
            subject_submitter_ids.append({"id": node.node_id, "submitter_id": node.props.get("submitter_id", None)})
        else:
            for link in node._pg_links:  
                tmp_dad = getattr(node, link)
                nodeType = link
                path_tmp = nodeType
                tmp = node._pg_links[link]["dst_type"] 
                while tmp.label != stop_node and tmp.label != "program":
                    # assuming ony one parents
                    nodeType = list(tmp._pg_links.keys())[0]
                    path_tmp = path_tmp + "." + nodeType 
                    tmp = tmp._pg_links[nodeType]["dst_type"]
                    # TODO double check this with deeper relationship > 2 nodes under project
                    tmp_dad = getattr(tmp_dad, nodeType)[0]

                if tmp.label == stop_node:
                    subject_submitter_ids.append({"id": tmp_dad[0].node_id, "submitter_id": tmp_dad[0].props.get("submitter_id", None)})
                else: 
                    logger.warn("resource not found " + node.label)
                    logger.warn(node)

    try:
        resources = [
                "/{}s/{}".format(stop_node, node["submitter_id"])
                for node in subject_submitter_ids
            ]
        authorize(program, project, [ROLES["READ"]], resources)
    except AuthZError:
        return "You do not have read permission on project {} for one or more of the subjects requested"


# TEST BUT YOU NEED TO ADD ACTUAL ID LIST NOT ONLY THE ONE LISTED IN THE DB
def get_authorized_ids(program, project):
    try:
        mapping = flask.current_app.auth.auth_mapping(current_user.username)
    except ArboristError as e:
        logger.warn(
            "Unable to retrieve auth mapping for user `{}`: {}".format(current_user.username, e)
        )
        mapping = {}

    base_resource_path = "/programs/{}/projects/{}".format(program, project)
    result = [resource_path for resource_path, permissions in mapping.items() if base_resource_path in resource_path]
    ids = []
    
    for path in result:
        parts = path.strip("/").split("/")
        if path != "/" and parts[0] != "programs":
            continue

        if len(parts) > 6 or (len(parts) > 2 and parts[2] != "projects") or (len(parts) > 4 and (flask.current_app.node_authz_entity_name is None or flask.current_app.node_authz_entity is None or parts[4] != (flask.current_app.node_authz_entity_name + "s"))):
            continue

        if len(parts) <  6:
            return(None)
            break
        else:
            ids.append(parts[5])

    return(ids)

