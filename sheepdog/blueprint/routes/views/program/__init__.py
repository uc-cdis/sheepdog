"""
View functions for routes in the blueprint for '/<program>' paths.
"""

import flask
import uuid

from sheepdog import auth
from sheepdog import models
from sheepdog.blueprint.routes.views.program import project
from sheepdog import utils
from sheepdog.errors import NotFoundError, UserError
from sheepdog.globals import PROJECT_SEED, ROLES
from sheepdog.transactions.upload.entity_factory import UploadEntityFactory
from sheepdog.transactions.upload.transaction import UploadTransaction


@utils.assert_program_exists
def get_projects(program):
    """
    Return the available resources at the top level of program ``program``,
    i.e. registered projects.

    Summary:
        Get the projects

    Tags:
        project

    Responses:
        200 (schema_links): Success
        403: Unauthorized request.
        404: Program not found.

    Args:
        program (str): |program_id|

    :reqheader Content-Type: |reqheader_Content-Type|
    :reqheader Accept: |reqheader_Accept|
    :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
    :resheader Content-Type: |resheader_Content-Type|

    **Example**

    .. code-block:: http

           GET /v0/submission/CGCI/ HTTP/1.1
           Host: example.com
           Content-Type: application/json
           X-Auth-Token: MIIDKgYJKoZIhvcNAQcC...
           Accept: application/json

    .. code-block:: JavaScript

        {
            "links": [
                "/v0/sumission/CGCI/BLGSP"
            ]
        }
    """
    if flask.current_app.config.get("AUTH_SUBMISSION_LIST", True) is True:
        auth.validate_request(
            scope={"openid"},
            audience=flask.current_app.config.get("OIDC_ISSUER")
            or flask.current_app.config.get("USER_API"),
            purpose=None,
        )
    with flask.current_app.db.session_scope():
        matching_programs = flask.current_app.db.nodes(models.Program).props(
            name=program
        )
        if not matching_programs.count():
            raise NotFoundError("program {} is not registered".format(program))
        projects = (
            flask.current_app.db.nodes(models.Project.code)
            .path("programs")
            .props(name=program)
            .all()
        )
    links = [
        flask.url_for(".create_entities", program=program, project=p[0])
        for p in projects
    ]
    return flask.jsonify({"links": links})


@utils.assert_program_exists
@auth.require_sheepdog_project_admin
def create_project(program):
    """
    Register a project.

    The content of the request is a JSON containing the information describing
    a project. Authorization for registering projects is limited to
    administrative users.

    Summary:
        Create a project

    Tags:
        project

    Args:
        program (str): |program_id|
        body (schema_project): input body

    Responses:
        200: Registered successfully.
        400: User error.
        404: Program not found.
        403: Unauthorized request.

    :reqheader Content-Type: |reqheader_Content-Type|
    :reqheader Accept: |reqheader_Accept|
    :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
    :resheader Content-Type: |resheader_Content-Type|

    Example:

        .. code-block:: http

            POST /v0/submission/CGCI/ HTTP/1.1
            Host: example.com
            Content-Type: application/json
            X-Auth-Token: MIIDKgYJKoZIhvcNAQcC...
            Accept: application/json

        .. code-block:: JavaScript

            {
                "type": "project",
                "code": "BLGSP",
                "disease_type": "Burkitt Lymphoma",
                "name": "Burkitt Lymphoma Genome Sequencing Project",
                "primary_site": "Lymph Nodes",
                "dbgap_accession_number": "phs000527",
                "state": "active"
            }
    """
    input_doc = flask.request.get_data().decode("utf-8")
    content_type = flask.request.headers.get("Content-Type", "").lower()
    errors = None
    if content_type == "text/csv":
        doc, errors = utils.transforms.CSVToJSONConverter().convert(input_doc)
    elif content_type in ["text/tab-separated-values", "text/tsv"]:
        doc, errors = utils.transforms.TSVToJSONConverter().convert(input_doc)
    else:
        doc = utils.parse.parse_request_json()

    if errors:
        raise UserError("Unable to parse doc '{}': {}".format(input_doc, errors))

    if isinstance(doc, list) and len(doc) == 1:
        # handle TSV/CSV submissions that are parsed as lists of 1 element
        doc = doc[0]
    if not isinstance(doc, dict):
        raise UserError(
            "The project creation endpoint only supports single documents (dict). Received data of type {}: {}".format(
                type(doc), doc
            )
        )
    if doc.get("type") and doc.get("type") not in ["project"]:
        raise UserError(
            "Invalid post to program endpoint with type='{}'".format(doc.get("type"))
        )
    # Parse project.code
    project = doc.get("code")
    if not project:
        raise UserError("No project specified in key 'code'")
    # Parse dbgap accession number.
    phsid = doc.get("dbgap_accession_number")
    if not phsid:
        raise UserError("No dbGaP accesion number specified.")

    # Create base JSON document.
    res = None
    with flask.current_app.db.session_scope() as session:
        program_node = utils.lookup_program(flask.current_app.db, program)
        if not program_node:
            raise NotFoundError("Program {} is not registered".format(program))
        # Look up project node.
        node = utils.lookup_project(flask.current_app.db, program, project)
        if not node:
            # Create a new project node
            node_uuid = str(uuid.uuid5(PROJECT_SEED, project))
            if flask.current_app.db.nodes(models.Project).ids(node_uuid).first():
                raise UserError(
                    "ERROR: Project {} already exists in DB".format(project)
                )

            node = models.Project(node_uuid)  # pylint: disable=not-callable
            node.programs = [program_node]
            action = "create"
            node.props["state"] = "open"
        else:
            action = "update"

        # silently drop system_properties
        doc.pop("type", None)
        doc.pop("state", None)
        doc.pop("released", None)

        try:
            node.props.update(doc)
        except AttributeError as e:
            raise UserError(f"ERROR: {e}")

        res_doc = dict(
            {"type": "project", "programs": {"id": program_node.node_id}}, **doc
        )

        # Create transaction
        transaction_args = dict(
            program=program,
            project=project,
            role=ROLES["UPDATE"],
            flask_config=flask.current_app.config,
        )
        with UploadTransaction(**transaction_args) as trans:
            node = session.merge(node)
            session.commit()
            entity = UploadEntityFactory.create(
                trans, doc=None, config=flask.current_app.config
            )
            entity.action = action
            entity.doc = res_doc
            entity.entity_type = "project"
            entity.unique_keys = node._secondary_keys_dicts
            entity.node = node
            entity.entity_id = entity.node.node_id
            trans.entities = [entity]
            res = flask.jsonify(trans.json)

        # create the resource in arborist
        auth.create_resource(program_node.dbgap_accession_number, phsid)

    return res


@utils.assert_program_exists
@auth.require_sheepdog_program_admin
def delete_program(program):
    """
    Delete a program given program name. If the program
    is not empty raise an appropriate exception

    Summary:
        Delete a program

    Tags:
        program

    Args:
        program (str): |program_id|

    Responses:
        204: Success.
        400: User error.
        404: Program not found.
        403: Unauthorized request.
    """
    with flask.current_app.db.session_scope() as session:
        node = utils.lookup_program(flask.current_app.db, program)
        if node.edges_in:
            raise UserError(
                "ERROR: Can not delete the program.\
                             Program {} is not empty".format(
                    program
                )
            )
        session.delete(node)
        session.commit()

        return flask.jsonify({}), 204


def create_transactions_viewer(operation, dry_run=False):

    # TODO TODO !!

    if operation == "close":
        pass
    elif operation == "commit":
        pass
