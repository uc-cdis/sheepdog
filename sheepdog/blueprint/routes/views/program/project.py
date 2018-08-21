# pylint: disable=protected-access
# pylint: disable=unsubscriptable-object
# pylint: disable=unsupported-membership-test
"""
View functions for routes in the blueprint for '/<program>/<project>' paths.
"""

import json

import flask
import sqlalchemy
import yaml

from sheepdog import auth
from sheepdog import dictionary
from sheepdog import models
from sheepdog import transactions
from sheepdog import utils
from sheepdog.errors import (
    AuthError,
    NotFoundError,
    UserError,
)
from sheepdog.globals import (
    PERMISSIONS,
    ROLES,
    STATES_COMITTABLE_DRY_RUN,
)


def create_viewer(method, bulk=False, dry_run=False):
    """
    Provide view functions for the following endpoints:

        /<program>/<project>
        /<program>/<project>/_dry_run
        /<program>/<project>/bulk
        /<program>/<project>/bulk/_dry_run

    for POST and PUT methods.

    The view function returned is for handling either a POST or PUT method and
    with ``dry_run`` being either True or False.
    """
    if method == 'POST':
        auth_roles = [ROLES['CREATE']]
        transaction_role = ROLES['CREATE']
    elif method == 'PUT':
        auth_roles = [ROLES['CREATE'], ROLES['UPDATE']]
        transaction_role = ROLES['UPDATE']
    else:
        # HCF
        raise RuntimeError('create_bulk_viewer: given invalid method')

    @utils.assert_project_exists
    @auth.authorize_for_project(*auth_roles)
    def create_entities(program, project):
        """
        Create GDC entities.

        Using the :http:method:`post` on a project's endpoint will create any
        valid entities specified in the request body.

        Args:
            program (str): |program_id|
            project (str): |project_id|

        :reqheader Content-Type: |reqheader_Content-Type|
        :reqheader Accept: |reqheader_Accept|
        :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
        :resheader Content-Type: |resheader_Content-Type|
        :statuscode 201: Entities created successfully
        :statuscode 404: Project not found.
        :statuscode 400: At least one entity was invalid.
        """
        return transactions.upload.handle_single_transaction(
            transaction_role, program, project, dry_run=dry_run
        )

    @utils.assert_project_exists
    @auth.authorize_for_project(*auth_roles)
    def bulk_create_entities(program, project):
        """
        Handle bulk transaction instead of single transaction; see
        documentation for the ``viewer`` function above.
        """
        return transactions.upload.handle_bulk_transaction(
            transaction_role, program, project, dry_run=dry_run
        )

    if bulk:
        return bulk_create_entities
    else:
        return create_entities


@utils.assert_project_exists
def get_project_dictionary(program=None, project=None):
    """
    Return links to the project level JSON schema definitions. (See
    :func:`get_dicionary`.)
    """
    if flask.current_app.config.get('AUTH_SUBMISSION_LIST', True) is True:
        auth.require_auth()
    keys = dictionary.schema.keys() + ['_all']
    links = [
        flask.url_for(
            '.get_project_dictionary_entry',
            program=program,
            project=project,
            entry=entry
        ) for entry in keys
    ]
    return flask.jsonify({'links': links})


def get_dictionary_entry(entry):
    """
    Return the project level JSON schema definition for a given entity
    type.

    Args:
        entry (str): entity type to retrieve the schema for (e.g. ``aliquot``)

    :reqheader Content-Type: |reqheader_Content-Type|
    :reqheader Accept: |reqheader_Accept|
    :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
    :resheader Content-Type: |resheader_Content-Type|
    :statuscode 200: Success
    :statuscode 404: Resource not found.
    :statuscode 403: Unauthorized request.

    **Example**

    .. code-block:: http

           GET /v0/submission/_dictionary/case HTTP/1.1
           Host: example.com
           Content-Type: application/json
           X-Auth-Token: MIIDKgYJKoZIhvcNAQcC...
           Accept: application/json

    .. code-block:: JavaScript

           {
             "$schema": "http://json-schema.org/draft-04/schema#",
             "additionalProperties": false,
             "category": "administrative",
             "description": "TODO",
             "id": "case",
             "links": [...]
             ...
           }
    """
    resolvers = {
        key.replace('.yaml', ''): resolver.source
        for key, resolver in dictionary.resolvers.iteritems()
    }
    if entry in resolvers:
        return flask.jsonify(resolvers[entry])
    elif entry == '_all':
        return flask.jsonify(dict(dictionary.schema, **resolvers))
    elif entry not in dictionary.schema:
        raise NotFoundError('Entry {} not in dictionary'.format(entry))
    else:
        return flask.jsonify(dictionary.schema[entry])


@utils.assert_program_exists
def get_project_dictionary_entry(program, project, entry):
    """
    Get the dictionary entry for a specific project. (See
    :func:`get_dictionary_entry`.)
    """
    if flask.current_app.config.get('AUTH_SUBMISSION_LIST', True) is True:
        auth.require_auth()
    return get_dictionary_entry(entry)


@utils.assert_program_exists
@auth.authorize_for_project(ROLES['READ'])
def get_entities_by_id(program, project, entity_id_string):
    """
    Retrieve existing GDC entities by ID.

    The return type of a :http:method:`get` on this endpoint is a JSON array
    containing JSON object elements, each corresponding to a provided ID.
    Return results are unordered.

    If any ID is not found in the database, a status code of 404 is returned
    with the missing IDs.

    Args:
        program (str): |program_id|
        project (str): |project_id|
        entity_id_string (str):
            A comma-separated list of ids specifying the entities to retrieve.

    :reqheader Content-Type: |reqheader_Content-Type|
    :reqheader Accept: |reqheader_Accept|
    :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
    :resheader Content-Type: |resheader_Content-Type|
    :statuscode 200: Success.
    :statuscode 404: Entity not found.
    :statuscode 403: Unauthorized request.
    """
    entity_ids = entity_id_string.split(',')
    with flask.current_app.db.session_scope():
        nodes = flask.current_app.db.nodes().ids(entity_ids).all()
        entities = {n.node_id: n for n in nodes}
        missing_entities = set(entity_ids) - set(entities.keys())
        if missing_entities:
            raise UserError(
                'Not found: {}'.format(', '.join(missing_entities), code=404)
            )
        return flask.jsonify(
            {'entities': utils.create_entity_list(entities.values())}
        )


def create_delete_entities_viewer(dry_run=False):
    """
    Create a view function for deleting entities.
    """

    @utils.assert_project_exists
    @auth.authorize_for_project(ROLES['DELETE'], ROLES['ADMIN'])
    def delete_entities(program, project, ids, to_delete=None):
        """
        Delete existing GDC entities.

        Using the :http:method:`delete` on a project's endpoint will
        *completely delete* an entity.

        The GDC does not allow deletions or creations that would leave nodes
        without parents, i.e. nodes that do not have an entity from which they
        were derived. To prevent catastrophic mistakes, the current philosophy
        is to disallow automatic cascading of deletes. However, to inform a
        user which entities must be deleted for the target entity to be
        deleted, the API will respond with a list of entities that must be
        deleted prior to deleting the target entity.

        :param str program: |program_id|
        :param str project: |project_id|
        :param str ids:
            A comma separated list of ids specifying the entities to delete.
            These ids must be official GDC ids.
        :param bool to_delete:
            Set the to_delete sysan as true or false. If none, then don't try
            to set the sysan, and instead delete the node.
        :param str ids:
        :reqheader Content-Type: |reqheader_Content-Type|
        :reqheader Accept: |reqheader_Accept|
        :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
        :resheader Content-Type: |resheader_Content-Type|
        :statuscode 200: Entities deleted successfully
        :statuscode 404: Entity not found.
        :statuscode 403: Unauthorized request.
        """

        ids_list = ids.split(',')
        fields = flask.request.args.get('fields')

        if to_delete is not None:
            # to_delete is admin only
            auth.current_user.require_admin()

            # get value of that flag from string
            if to_delete.lower() == 'false':
                to_delete = False
            elif to_delete.lower() == 'true':
                to_delete = True
            else:
                raise UserError('to_delete value not true or false')

        return transactions.deletion.handle_deletion_request(
            program,
            project,
            ids_list,
            to_delete,
            dry_run=dry_run,
            fields=fields
        )

    return delete_entities


@auth.authorize_for_project(ROLES['READ'])
def export_entities(program, project):
    """
    Return a file with the requested entities as an attachment.

    Omitting the ``ids`` parameter from the query string will return _ALL_
    nodes under the given project.

    Otherwise, if there is only one entity type in the output, it will return a
    {node_type}.tsv or {node_type}.json file, e.g.: aliquot.tsv. If there are
    multiple entity types, it returns ``gdc_export_{one_time_sha}.tar.gz`` for
    tsv format, or ``gdc_export_{one_time_sha}.json`` for json format.

    Args:
        program (str)
        project (str)

    Query Parameters:
        ids: one or a list of gdc ids seperated by commas.
        format: output format, ``json`` or ``tsv`` or ``csv``; default is tsv
        with_children:
            whether to recursively find children or not; default is False
        category: category of node to filter on children. Example: clinical

    Status Codes:
        200: Success
        400: Bad Request
        404: No id is found
        403: Unauthorized
    """
    if flask.request.method == 'GET':
        # Unpack multidict, or values will unnecessarily be lists.
        kwargs = {k: v for k, v in flask.request.args.iteritems()}
    else:
        kwargs = utils.parse.parse_request_json()

    # Convert `format` argument to `file_format`.
    if 'format' in kwargs:
        kwargs['file_format'] = kwargs['format']
        del kwargs['format']

    node_label = kwargs.get('node_label')
    project_id = '{}-{}'.format(program, project)
    file_format = kwargs.get('file_format') or 'tsv'

    mimetype = 'application/octet-stream'
    if not kwargs.get('ids'):
        if not node_label:
            raise UserError('expected either `ids` or `node_label` parameter')
        filename = '{}.{}'.format(node_label, file_format)
        content_disp = 'attachment; filename={}'.format(filename)
        headers = {'Content-Disposition': content_disp}
        utils.transforms.graph_to_doc.validate_export_node(node_label)
        return flask.Response(
            utils.transforms.graph_to_doc.export_all(
                node_label, project_id, file_format, flask.current_app.db
            ),
            mimetype=mimetype,
            headers=headers,
        )
    else:
        output = utils.transforms.graph_to_doc.ExportFile(
            program=program, project=project, **kwargs
        )
        content_disp = 'attachment; filename={}'.format(output.filename)
        headers = {'Content-Disposition': content_disp}
        return flask.Response(
            output.get_response(), mimetype=mimetype, headers=headers
        )


def create_files_viewer(dry_run=False, reassign=False):
    """
    Create a view function for handling file operations.
    """
    auth_roles = [
        ROLES['CREATE'], ROLES['UPDATE'], ROLES['DELETE'], ROLES['DOWNLOAD'],
        ROLES['READ'], ROLES['ADMIN'],
    ]

    @utils.assert_project_exists
    @auth.authorize_for_project(*auth_roles)
    def file_operations(program, project, file_uuid):
        """
        Handle molecular file operations.  This will only be available once the
        user has created a file entity with GDC id ``uuid`` via the
        ``/<program>/<project>/`` endppoint.

        This endpoint is an S3 compatible endpoint as described here:
        http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectOps.html

        Supported operations:

        PUT /<program>/<project>/files/<uuid>
            Upload data using single PUT. The request body should contain
            binary data of the file

        PUT /internal/<program>/<project>/files/<uuid>/reassign
            Manually (re)assign the S3 url for a given node

        DELETE /<program>/<project>/files/<uuid>
            Delete molecular data from object storage.

        POST /<program>/<project>/files/<uuid>?uploads
            Initiate Multipart Upload.

        PUT /<program>/<project>/files/<uuid>?partNumber=PartNumber&uploadId=UploadId
            Upload Part.

        POST /<program>/<project>/files/<uuid>?uploadId=UploadId
            Complete Multipart Upload

        DELETE /<program>/<project>/files/<uuid>?uploadId=UploadId
            Abort Multipart Upload

        GET /<program>/<project>/files/<uuid>?uploadId=UploadId
            List Parts

        :param str program: |program_id|
        :param str project: |project_id|
        :param str uuid: The GDC id of the file to upload.
        :reqheader Content-Type: |reqheader_Content-Type|
        :reqheader Accept: |reqheader_Accept|
        :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
        :resheader Content-Type: |resheader_Content-Type|
        :statuscode 200: Success.
        :statuscode 404: File not found.
        :statuscode 403: Unauthorized request.
        :statuscode 405: Method Not Allowed.
        :statuscode 400: Bad Request.
        """
        headers = {
            k: v
            for k, v in flask.request.headers.iteritems()
            if v and k != 'X-Auth-Token'
        }
        url = flask.request.url.split('?')
        args = url[-1] if len(url) > 1 else ''
        if flask.request.method == 'GET':
            if flask.request.args.get('uploadId'):
                action = 'list_parts'
            else:
                raise UserError('Method GET not allowed on file', code=405)
        elif flask.request.method == 'POST':
            if flask.request.args.get('uploadId'):
                action = 'complete_multipart'
            elif flask.request.args.get('uploads') is not None:
                action = 'initiate_multipart'
            else:
                action = 'upload'
        elif flask.request.method == 'PUT':
            if reassign:
                # admin only
                auth.current_user.require_admin()
                action = 'reassign'
            elif flask.request.args.get('partNumber'):
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
        roles = auth.get_program_project_roles(*project_id.split('-'))
        if role not in roles:
            raise AuthError(
                "You don't have {} role to do '{}'".format(role, action)
            )

        resp = utils.proxy_request(
            project_id,
            file_uuid,
            flask.request.stream,
            args,
            headers,
            flask.request.method,
            action,
            dry_run,
        )


        if dry_run or action == 'reassign':
            return resp

        return flask.Response(
            resp.read(), status=resp.status, headers=resp.getheaders(),
            mimetype='text/xml'
        )

    return file_operations


@auth.authorize_for_project(ROLES['READ'])
def get_manifest(program, project):
    id_string = flask.request.args.get('ids', '').strip()
    if not id_string:
        raise UserError(
            "No ids specified. Use query parameter 'ids', e.g."
            " 'ids=id1,id2'."
        )
    requested_ids = id_string.split(',')
    docs = utils.manifest.get_manifest(program, project, requested_ids)
    response = flask.make_response(
        yaml.safe_dump({'files': docs }, default_flow_style=False)
    )
    filename = "submission_manifest.yaml"
    response.headers["Content-Disposition"] = (
        "attachment; filename={}".format(filename))
    return response


def create_open_project_viewer(dry_run=False):

    @utils.assert_project_exists
    @auth.authorize_for_project(ROLES['RELEASE'])
    def open_project(program, project):
        """
        Mark a project ``open``. Opening a project means uploads, deletions, etc.
        are allowed.

        :param str program: |program_id|
        :param str project: |project_id|
        :reqheader Content-Type: |reqheader_Content-Type|
        :reqheader Accept: |reqheader_Accept|
        :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
        :statuscode 200: Project submitted successfully
        :statuscode 404: Project not found.
        :statuscode 403: Unauthorized request.
        """
        return transactions.review.handle_open_transaction(program, project, dry_run=dry_run)

    return open_project


def create_release_project_viewer(dry_run=False):
    """
    Create a view function for ``/<program>/<project>/open`` or
    ``/<program>/<project>/open/_dry_run``.
    """

    @utils.assert_project_exists
    @auth.authorize_for_project(ROLES['RELEASE'])
    def release_project(program, project):
        """
        Release a project.

        :param str program: |program_id|
        :param str project: |project_id|
        :reqheader Content-Type: |reqheader_Content-Type|
        :reqheader Accept: |reqheader_Accept|
        :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
        :statuscode 200: Project submitted successfully
        :statuscode 404: Project not found.
        :statuscode 403: Unauthorized request.
        """
        return transactions.release.handle_release_transaction(program, project, dry_run=dry_run)

    return release_project


def create_review_project_viewer(dry_run=False):
    """
    TODO: Docstring for create_review_project_viewer.
    :param dry_run: TODO
    :return: TODO
    """

    @utils.assert_project_exists
    @auth.authorize_for_project(ROLES['RELEASE'])
    def review_project(program, project):
        """
        Mark a project project for review.

        Reviewing a project means uploads are locked. An ``open`` or ``submit``
        action must be taken after ``review``.

        :param str program: |program_id|
        :param str project: |project_id|
        :reqheader Content-Type: |reqheader_Content-Type|
        :reqheader Accept: |reqheader_Accept|
        :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
        :statuscode 200: Project submitted successfully
        :statuscode 404: Project not found.
        :statuscode 403: Unauthorized request.
        """
        return transactions.review.handle_review_transaction(program, project, dry_run=dry_run)

    return review_project


def create_submit_project_viewer(dry_run=False):
    """
    TODO: Docstring for create_submit_projecct_viewer.
    :param dry_run: TODO
    :return: TODO
    """

    @utils.assert_project_exists
    @auth.authorize_for_project(ROLES['RELEASE'])
    def submit_project(program, project):
        """
        Submit a project.

        Submitting a project means that the GDC can make all metadata that
        *currently* exists in the project public in every GDC index built after
        the project is released.

        :param str program: |program_id|
        :param str project: |project_id|
        :reqheader Content-Type: |reqheader_Content-Type|
        :reqheader Accept: |reqheader_Accept|
        :reqheader X-Auth-Token: |reqheader_X-Auth-Token|
        :statuscode 200: Project submitted successfully
        :statuscode 404: Project not found.
        :statuscode 403: Unauthorized request.
        """
        return transactions.submission.handle_submission_transaction(program, project, dry_run=dry_run)

    return submit_project


@utils.assert_project_exists
def get_project_templates(program, project):
    """
    Get templates for all entity types.

    In the template, Links are represented as {link_type}.{link_unique_key} for
    one_to_one  and many_to_one relationships. For many_to_many relationships,
    they are represented as {link_type}.{link_unique_key}#1 to infer user the
    multiplicity

    :param str program: |program_id|
    :param str project: |project_id|
    :query format: output format, ``csv`` or ``tsv``, default is tsv
    :query categories: target entities' categories
    :query exclude: entity types to be excluded
    :statuscode 200 Success
    """
    file_format = flask.request.args.get('format', 'tsv')
    template = utils.transforms.graph_to_doc.get_all_template(
        file_format, program=program, project=project,
        categories=flask.request.args.get('categories'),
        exclude=flask.request.args.get('exclude')
    )
    response = flask.make_response(template)
    suffix = 'json' if file_format == 'json' else 'tar.gz'
    response.headers['Content-Disposition'] = (
        'attachment; filename=submission_templates.{}'.format(suffix)
    )
    return response


@utils.assert_project_exists
def get_project_template(program, project, entity):
    """
    Return TSV template of an entity type.

    In the template, links are represented as {link_type}.{link_unique_key} for
    one_to_one and many_to_one relationships. For many_to_many relationships,
    they are represented as {link_type}.{link_unique_key}#1 to infer user the
    multiplicity.

    :param str program: |program_id|
    :param str project: |project_id|
    :param str entity: type of the entity
    :query format: output format, ``csv`` or ``tsv``, default is tsv
    :statuscode 200 Success
    :statuscode 404 Entity type is not found
    """
    file_format = flask.request.args.get('format', 'tsv')
    template = utils.entity_to_template_str(
        entity, file_format, program=program, project=project
    )
    filename = "submission_{}_template.{}".format(entity, file_format)
    response = flask.make_response(template)
    response.headers["Content-Disposition"] = (
        "attachment; filename={}".format(filename)
    )
    response.headers["Content-Type"] = "application/octet-stream"
    return response


@utils.assert_project_exists
@auth.authorize_for_project(ROLES['UPDATE'])
def close_transaction(program, project, transaction_id):
    """
    Close a transaction. The transaction is prevented from being committed in
    the future.
    """
    with flask.current_app.db.session_scope():
        try:
            tx_log = (
                flask.current_app.db.nodes(models.submission.TransactionLog)
                .filter(models.submission.TransactionLog.id == transaction_id)
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound:
            project_id = '{}-{}'.format(program, project)
            raise NotFoundError(
                'Unable to find transaction_log with id {} for project {}'
                .format(transaction_id, project_id)
            )
        # Check if already closed.
        if tx_log.closed:
            raise UserError("This transaction log is already closed.")
        # Check if dry_run.
        if tx_log.is_dry_run is False:
            raise UserError("This transaction log is not a dry run. "
                            "Closing it would have no effect.")
        # Check if already committed.
        if tx_log.committed_by is not None:
            raise UserError("This transaction log has already been committed. "
                            "Closing it would have no effect.")
        tx_log.closed = True

    return flask.jsonify({
        'code': 200,
        'message': 'Closed transaction.',
        'transaction_id': transaction_id,
    })


def resubmit_transaction(transaction_log):
    """Create a new transaction based on existing transaction_log."""
    program, project = transaction_log.program, transaction_log.project

    if transaction_log.role in {'create', 'update'}:
        return transactions.upload._single_transaction(
            transaction_log.role,
            program,
            project,
            None,
            'json',
            json.dumps(transaction_log.canonical_json),
            transaction_log.canonical_json,
            dry_run=False
        )
    elif transaction_log.role in {'delete'}:
        return transactions.deletion.handle_deletion_request(
            program,
            project,
            [entity.node_id for entity in transaction_log.entities],
            dry_run=False
        )
    elif transaction_log.role in {'review'}:
        return transactions.review.handle_review_transaction(
            program,
            project,
            dry_run=False
        )
    elif transaction_log.role in {'open'}:
        return transactions.review.handle_open_transaction(
            program,
            project,
            dry_run=False
        )
    elif transaction_log.role in {'release'}:
        return transactions.release.handle_release_transaction(
            program,
            project,
            dry_run=False
        )
    elif transaction_log.role in {'submit'}:
        return transactions.submission.handle_submission_transaction(
            program,
            project,
            dry_run=False
        )


@utils.assert_project_exists
@auth.authorize_for_project(
    ROLES['CREATE'], ROLES['UPDATE'], ROLES['DELETE'], ROLES['RELEASE'],
    'review', 'submit'
)
def commit_dry_run_transaction(program, project, transaction_id):
    """
    See documentation for committing a dry run transaction.

    This call should only succeed if:
    1. transaction_id points to a dry_run transaction
    2. transaction_id points to a transaction that hasn't been committed already
    3. transaction_id points to a successful transaction
    """
    with flask.current_app.db.session_scope():
        try:
            tx_log = (
                flask.current_app.db.nodes(models.submission.TransactionLog)
                .filter(models.submission.TransactionLog.id == transaction_id)
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound:
            raise NotFoundError(
                'Unable to find transaction_log with id: {} for project {}'
                .format(transaction_id, '{}-{}'.format(program, project))
            )
        # Check state.
        if tx_log.state not in STATES_COMITTABLE_DRY_RUN:
            raise UserError(
                'Unable to commit transaction log in state {}.'
                .format(tx_log.state)
            )
        # Check not closed.
        if tx_log.closed:
            raise UserError('Unable to commit closed transaction log.')
        # Check not committed.
        if tx_log.committed_by is not None:
            raise UserError(
                "This transaction_log was committed already by transaction "
                "'{}'.".format(tx_log.committed_by)
            )
        # Check is dry_run
        if tx_log.is_dry_run is not True:
            raise UserError(
                "Cannot submit transaction_log '{}', not a dry_run."
                .format(tx_log.id)
            )
        # Check project
        if tx_log.project != project or tx_log.program != program:
            raise UserError(
                "Cannot submit transaction_log '{}', in project {}-{}."
                .format(tx_log.id, program, project)
            )

        response, code = resubmit_transaction(tx_log)
        response_data = json.loads(response.get_data())
        tx_log.committed_by = response_data['transaction_id']

        return response, code


def create_biospecimen_viewer(dry_run=False):

    @utils.assert_project_exists
    @auth.authorize_for_project(ROLES['CREATE'], ROLES['UPDATE'])
    def update_entities_biospecimen_bcr(program, project):
        return transactions.upload.handle_biospecimen_bcr_xml_transaction(
            ROLES['UPDATE'],
            program,
            project,
            dry_run=dry_run,
        )

    return update_entities_biospecimen_bcr


def create_clinical_viewer(dry_run=False):

    @utils.assert_project_exists
    @auth.authorize_for_project(ROLES['CREATE'], ROLES['UPDATE'])
    def update_entities_clinical_bcr(program, project):
        return transactions.upload.handle_clinical_bcr_xml_transaction(
            ROLES['UPDATE'], program, project, dry_run=dry_run
        )

    return update_entities_clinical_bcr


@utils.assert_project_exists
def delete_project(program, project):
    """
    Delete project under a specific program
    """
    auth.current_user.require_admin()
    with flask.current_app.db.session_scope() as session:
        node = utils.lookup_project(flask.current_app.db, program, project)
        if node.edges_in:
            raise UserError('ERROR: Can not delete the project.\
                             Project {} is not empty'.format(project))
        transaction_args = dict(
            program=program,
            project=project,
            flask_config=flask.current_app.config
        )
        with (
                transactions.deletion.transaction.
                DeletionTransaction(**transaction_args)
             ) as trans:
            session.delete(node)
            trans.claim_transaction_log()
            trans.write_transaction_log()
            session.commit()
            return flask.jsonify(trans.json), 204
