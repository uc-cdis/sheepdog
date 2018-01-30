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
from sheepdog.transactions.upload.entity import UploadEntity
from sheepdog.transactions.upload.transaction import UploadTransaction


@utils.assert_program_exists
def get_projects(program):
    """
    Return the available resources at the top level of program ``program``,
    i.e. registered projects.

    Args:
        program (str): |program_id|

    :reqheader Content-Type: |reqheader_Content-Type|
    :reqheader Accept: |reqheader_Accept|
    :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
    :resheader Content-Type: |resheader_Content-Type|
    :statuscode 200: Success
    :statuscode 403: Unauthorized request.
    :statuscode 404: Program not found.

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
        auth.require_auth()
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

    Args:
        program (str): |program_id|

    :reqheader Content-Type: |reqheader_Content-Type|
    :reqheader Accept: |reqheader_Accept|
    :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
    :resheader Content-Type: |resheader_Content-Type|
    :statuscode 200: Registered successfully.
    :statuscode 404: Program not found.
    :statuscode 403: Unauthorized request.

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
    auth.admin_auth()
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
            entity = UploadEntity(trans, flask.current_app.config)
            entity.action = action
            entity.doc = doc
            entity.entity_type = 'project'
            entity.unique_keys = node._secondary_keys_dicts
            entity.node = node
            entity.entity_id = entity.node.node_id
            flask_config=flask.current_app.config,
            trans.entities = [entity]
            return flask.jsonify(trans.json)


def create_files_viewer(dry_run=False):
    auth_roles = [
        ROLES['CREATE'], ROLES['UPDATE'], ROLES['DELETE'], ROLES['DOWNLOAD'],
        ROLES['READ']
    ]

    @utils.assert_project_exists
    @auth.authorize_for_project(*auth_roles)
    def files_viewer(program, project, file_uuid):
        headers = {
            k: v
            for k, v in flask.request.headers.iteritems()
            if v and k != 'X-Auth-Token'
        }
        url = flask.request.url.split('?')
        args = url[-1] if len(url) > 1 else ""
        if flask.request.method == 'GET':
            if flask.request.args.get('uploadId'):
                action = 'list_parts'
            else:
                raise UserError("Method GET not allowed on file", code=405)
        elif flask.request.method == 'POST':
            if flask.request.args.get('uploadId'):
                action = 'complete_multipart'
            elif flask.request.args.get('uploads') is not None:
                action = 'initiate_multipart'
            else:
                action = 'upload'
        elif flask.request.method == 'PUT':
            if flask.request.args.get('partNumber'):
                action = 'upload_part'
            else:
                action = 'upload'
        elif flask.request.method == 'DELETE':
            if flask.request.args.get('uploadId'):
                action = 'abort_multipart'
            else:
                action = 'delete'
        else:
            raise UserError('Unsupported file operation', code=405)


        project_id = program + '-' + project
        role = PERMISSIONS[action]
        if role not in flask.g.user.roles[project_id]:
            raise AuthError(
                "You don't have {} role to do '{}'".format(role, action)
            )

        resp = utils.proxy_request(
            project_id, file_uuid, flask.request.stream, args, headers,
            flask.request.method, action, dry_run
        )

        if dry_run:
            return resp

        return flask.Response(
            resp.read(), status=resp.status, headers=resp.getheaders(),
            mimetype='text/xml'
        )


def create_transactions_viewer(operation, dry_run=False):

    # TODO TODO !!

    if operation == 'close':
        pass
    elif operation == 'commit':
        pass
