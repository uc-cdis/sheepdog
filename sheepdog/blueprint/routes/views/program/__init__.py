"""
View functions for routes in the blueprint for '/<program>' paths.
"""

import json
import uuid

import flask
import sqlalchemy
import yaml

from sheepdog import auth
from sheepdog import dictionary
from sheepdog import models
from sheepdog import utils
from sheepdog.blueprint.routes.views.program import project
from sheepdog.errors import (
    APINotImplemented,
    AuthError,
    NotFoundError,
    UserError,
)
from sheepdog.globals import (
    PERMISSIONS,
    PROJECT_SEED,
    ROLES,
    STATES_COMITTABLE_DRY_RUN,
)
from sheepdog.transactions import upload
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
    if flask.current_app.config.get('AUTH_SUBMISSION_LIST', True) is True:
        auth.validate_request(aud={'openid'}, purpose=None)
    with flask.current_app.db.session_scope():
        matching_programs = (
            flask.current_app
            .db
            .nodes(models.Program)
            .props(name=program)
        )
        if not matching_programs.count():
            raise NotFoundError('program {} is not registered'.format(program))
        projects = (
            flask.current_app
            .db
            .nodes(models.Project.code)
            .path('programs')
            .props(name=program)
            .all()
        )
    links = [
        flask.url_for('.create_entities', program=program, project=p[0])
        for p in projects
    ]
    return flask.jsonify({'links': links})


@utils.assert_program_exists
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
        program (string): |program_id|
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
    auth.current_user.require_admin()
    doc = utils.parse.parse_request_json()
    if not isinstance(doc, dict):
        raise UserError('Program endpoint only supports single documents')
    if doc.get('type') and doc.get('type') not in ['project']:
        raise UserError("Invalid post to program endpoint with type='{}'"
                        .format(doc.get('type')))
    # Parse project.code
    project = doc.get('code')
    if not project:
        raise UserError("No project specified in key 'code'")
    project = project.encode('utf-8')
    # Parse dbgap accession number.
    phsid = doc.get('dbgap_accession_number')
    if not phsid:
        raise UserError("No dbGaP accesion number specified.")

    # Create base JSON document.
    base_doc = utils.parse.parse_request_json()
    with flask.current_app.db.session_scope() as session:
        program_node = utils.lookup_program(flask.current_app.db, program)
        if not program_node:
            raise NotFoundError('Program {} is not registered'.format(program))
        # Look up project node.
        node = utils.lookup_project(flask.current_app.db, program, project)
        if not node:
            # Create a new project node
            node_uuid = str(uuid.uuid5(PROJECT_SEED, project.encode('utf-8')))
            if (
                flask.current_app
                .db.nodes(models.Project)
                .ids(node_uuid)
                .first()
            ):
                raise UserError('ERROR: Project {} already exists in DB'
                                .format(project))

            node = models.Project(node_uuid)  # pylint: disable=not-callable
            node.programs = [program_node]
            action = 'create'
            node.props['state'] = 'open'
        else:
            action = 'update'

        # silently drop system_properties
        base_doc.pop('type', None)
        base_doc.pop('state', None)
        base_doc.pop('released', None)

        node.props.update(base_doc)

        doc = dict({
            'type': 'project',
            'programs': {'id': program_node.node_id},
        }, **base_doc)

        # Create transaction
        transaction_args = dict(
            program=program,
            project=project,
            role=ROLES['UPDATE'],
            flask_config=flask.current_app.config
        )
        with UploadTransaction(**transaction_args) as trans:
            node = session.merge(node)
            session.commit()
            entity = UploadEntityFactory.create(
                trans, doc=None, config=flask.current_app.config)
            entity.action = action
            entity.doc = doc
            entity.entity_type = 'project'
            entity.unique_keys = node._secondary_keys_dicts
            entity.node = node
            entity.entity_id = entity.node.node_id
            trans.entities = [entity]
            return flask.jsonify(trans.json)


@utils.assert_program_exists
def delete_program(program):
    """
    Delete a program given program name. If the program
    is not empty raise an appropriate exception

    Summary:
        Delete a program
        
    Tags:
        program

    Args:
        program (string): |program_id|
    
    Responses:
        204: Success.
        400: User error.
        404: Program not found.
        403: Unauthorized request.
    """
    auth.current_user.require_admin()
    with flask.current_app.db.session_scope() as session:
        node = utils.lookup_program(flask.current_app.db, program)
        if node.edges_in:
            raise UserError('ERROR: Can not delete the program.\
                             Program {} is not empty'.format(program))
        session.delete(node)
        session.commit()

        return flask.jsonify({}), 204


def create_transactions_viewer(operation, dry_run=False):

    # TODO TODO !!

    if operation == 'close':
        pass
    elif operation == 'commit':
        pass
