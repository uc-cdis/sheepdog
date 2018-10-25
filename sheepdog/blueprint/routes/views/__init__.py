"""
Provide view functions for routes in the blueprint.
"""

import uuid

import flask
from flask import current_app
import requests

from sheepdog import auth, dictionary, models, utils
from sheepdog.utils import manifest, parse
from sheepdog.blueprint.routes.views import program
from sheepdog.errors import (
    AuthError,
    NotFoundError,
    UserError,
)
from sheepdog.globals import (
    PROGRAM_SEED,
    ROLES,
)


def get_programs():
    """
    /
    GET

    Return the available resources at the top level above programs i.e.
    registered programs.

    :reqheader Content-Type: |reqheader_Content-Type|
    :reqheader Accept: |reqheader_Accept|
    :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
    :resheader Content-Type: |resheader_Content-Type|
    :statuscode 200: Success
    :statuscode 404: Program not found.
    :statuscode 403: Unauthorized request.

    **Example**

    .. code-block:: http

           GET /v0/submission/ HTTP/1.1
           Host: example.com
           Content-Type: application/json
           X-Auth-Token: MIIDKgYJKoZIhvcNAQcC...
           Accept: application/json

    .. code-block:: JavaScript

        {
            "links": [
                "/v0/sumission/CGCI/",
                "/v0/sumission/TARGET/",
                "/v0/sumission/TCGA/"
            ]
        }
    """
    if flask.current_app.config.get('AUTH_SUBMISSION_LIST', True) is True:
        auth.validate_request(aud={'openid'}, purpose=None)
    with flask.current_app.db.session_scope():
        programs = current_app.db.nodes(models.Program.name).all()
    links = [flask.url_for('.get_projects', program=p[0]) for p in programs]
    return flask.jsonify({'links': links})


def root_create():
    """
    /
    PUT POST PATCH

    Register a program.

    The content of the request is a JSON containing the information
    describing a program.  Authorization for registering programs is
    limited to administrative users.

    :reqheader Content-Type:
        |reqheader_Content-Type|
    :reqheader Accept:
        |reqheader_Accept|
    :reqheader X-Auth-Token:
        |reqheader_X-Auth-Token|
    :resheader Content-Type:
        |resheader_Content-Type|
    :statuscode 200: Registered successfully.
    :statuscode 404: Program not found.
    :statuscode 403: Unauthorized request.

    **Example:**

    .. code-block:: http

           POST /v0/submission/CGCI/ HTTP/1.1
           Host: example.com
           Content-Type: application/json
           X-Auth-Token: MIIDKgYJKoZIhvcNAQcC...
           Accept: application/json

    .. code-block:: JavaScript

        {
            "type": "program",
            "name": "CGCI",
            "dbgap_accession_number": "phs000178"
        }
    """
    auth.current_user.require_admin()
    message = None
    node_id = None
    doc = parse.parse_request_json()
    if not isinstance(doc, dict):
        raise UserError('Root endpoint only supports single documents')
    if doc.get('type') != 'program':
        raise UserError(
            "Invalid type in key type='{}'".format(doc.get('type'))
        )
    phsid = doc.get('dbgap_accession_number')
    program = doc.get('name')
    if not program:
        raise UserError("No program specified in key 'name'")

    with current_app.db.session_scope(can_inherit=False) as session:
        node = (
            current_app
            .db
            .nodes(models.Program)
            .props(name=program)
            .scalar()
        )
        if node:
            message = 'Program is updated!'
            node_id = node.node_id
            node.props['dbgap_accession_number'] = phsid
        else:
            node_id = str(uuid.uuid5(PROGRAM_SEED, program.encode('utf-8')))
            session.add(models.Program(  # pylint: disable=not-callable
                node_id, name=program, dbgap_accession_number=phsid
            ))
            message = 'Program registered.'

    return flask.jsonify({
        'id': node_id,
        'name': program,
        'message': message,
    })


def get_dictionary():
    """
    /_dictionary
    GET

    Return links to the JSON schema definitions.

    :statuscode 200: Success
    :statuscode 403: Unauthorized request.

    **Example**

    .. code-block:: http

           GET /v0/submission/CGCI/BLGSP/_dictionary HTTP/1.1
           Host: example.com
           Content-Type: application/json
           X-Auth-Token: MIIDKgYJKoZIhvcNAQcC...
           Accept: application/json

    .. code-block:: JavaScript

           {
             "links": [
               "/v0/submission/_dictionary/case",
               "/v0/submission/_dictionary/program",
               "/v0/submission/_dictionary/aliquot",
               "/v0/submission/_dictionary/annotation",
               ...
             ]
           }
    """
    keys = dictionary.schema.keys() + ['_all']
    links = [
        flask.url_for('.get_dictionary_entry', entry=entry)
        for entry in keys
    ]
    return flask.jsonify({'links': links})


def get_templates():
    """
    get_templates

    :statuscode 200: Success.
    """
    file_format = flask.request.args.get('format', 'tsv')
    response = flask.make_response(utils.get_all_template(
        file_format,
        categories=flask.request.args.get('categories'),
        exclude=flask.request.args.get('exclude')
    ))
    suffix = 'json' if file_format == 'json' else 'tar.gz'
    response.headers['Content-Disposition'] = (
        'attachment; filename=submission_templates.{}'.format(suffix)
    )
    return response


def get_template(entity):
    """
    get_templates

    :param str entity: entity

    :statuscode 200: Success.
    """
    file_format = flask.request.args.get('format', 'tsv')
    filename = "submission_{}_template.{}".format(entity, file_format)
    template = utils.entity_to_template_str(entity, file_format)
    response = flask.make_response(template)
    content_disposition = "attachment; filename={}".format(filename)
    response.headers["Content-Disposition"] = content_disposition
    response.headers["Content-Type"] = "application/octet-stream"
    return response


def validate_upload_manifest():
    """
    validate_upload_manifest

    :statuscode 200: Success.
    """
    content_type = flask.request.headers.get('Content-Type', '').lower()
    if content_type == 'application/json':
        manifest_doc = parse.parse_request_json()
    else:
        manifest_doc = parse.parse_request_yaml()
    errors = manifest.validate_upload_manifest(manifest_doc)
    if errors:
        return flask.jsonify({"valid": False, "errors": errors})
    else:
        return flask.jsonify({"valid": True})
