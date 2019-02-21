# pylint: disable=unsubscriptable-object
"""
Provide utility functions primarily for code in ``sheepdog.blueprint``
(though some are also used in ``sheepdog.upload``).
"""

import copy
import functools
import json
import StringIO
import tarfile
import time
from contextlib import contextmanager
from urlparse import urlparse

import flask
from fuzzywuzzy.process import extract

from sheepdog import dictionary, models
from sheepdog.errors import InternalError, NotFoundError, UserError
from sheepdog.globals import (
    DATA_FILE_CATEGORIES,
    ERROR_STATE,
    PRIMARY_URL_TYPE,
    STATES_COMITTABLE_DRY_RUN,
    TEMPLATE_NAME,
    UPLOADED_STATE,
    UPLOADING_PARTS,
    UPLOADING_STATE,
    submitted_state,
    DELETE_STATE
)
from sheepdog.utils.transforms.graph_to_doc import (
    entity_to_template,
    entity_to_template_str,
)

from . import s3

ALLOWED_STATES_FOR_UPLOAD = [
    ERROR_STATE,
    submitted_state(),
    UPLOADING_STATE,
    UPLOADED_STATE,
    DELETE_STATE
]


def _get_links(file_format, schema, exclude_id):
    """
    Parse links from schema.

    we don't have project specific schema now
    so right now this uses top level schema

    TODO
    """
    links = dict()
    subgroups = [link for link in schema if 'subgroup' in link]
    non_subgroups = [link for link in schema if 'name' in link]
    for link in non_subgroups:
        if file_format == 'json':
            links[link['name']] = _get_links_json(link, exclude_id)
        else:
            links[link['name']] = _get_links_delimited(link, exclude_id)
    for subgroup in subgroups:
        links.update(_get_links(file_format, subgroup['subgroup'], exclude_id))
    return links


def _get_links_json(link, exclude_id):
    """
    Return parsed link template from link schema in json form.
    """
    target_schema = dictionary.schema[link['target_type']]
    link_template = dict({
        k: None
        for subkeys in target_schema.get('uniqueKeys', [])
        for k in subkeys
    })
    if 'project_id' in link_template:
        del link_template['project_id']
    if exclude_id:
        del link_template['id']
    return link_template


def _get_links_delimited(link, exclude_id):
    """
    Return parsed link template from link schema in delimited form.
    """
    link_template = []
    target_schema = dictionary.schema[link['target_type']]
    # Add a #1 to the link to indicate it's a many relationship.
    to_many = link['multiplicity'] in ['many_to_many', 'one_to_many']
    postfix = "#1" if to_many else ""
    # default key for link is the GDC ID
    if not exclude_id:
        link_template.append('id' + postfix)
    unique_keys = (key for key in target_schema['uniqueKeys'] if key != ['id'])
    for unique_key in unique_keys:
        keys = copy.copy(unique_key)
        if 'project_id' in keys:
            keys.remove('project_id')
        link_template += [prop + postfix for prop in keys]

        # right now we only have one alias for each entity,
        # so we pick the first one for now
        break
    return link_template


def assert_program_exists(func):
    """
    Wrap a function to check that a Program node with a matching name exists.
    """
    @functools.wraps(func)
    def check(program, *args, **kwargs):
        with flask.current_app.db.session_scope():
            programs = (
                flask.current_app
                .db
                .nodes(models.Program)
                .props(name=program)
            )
            if not programs.count():
                raise NotFoundError('program {} not found'.format(program))
        return func(program, *args, **kwargs)
    return check


def assert_project_exists(func):
    """
    Wrap a function to check that a Project node with a matching name exists.

    TODO
    """
    @functools.wraps(func)
    def check_and_call(program, project, *args, **kwargs):
        with flask.current_app.db.session_scope():
            # Check that the program exists
            program_node = (
                flask.current_app
                .db
                .nodes(models.Program)
                .props(name=program)
                .first()
            )
            if not program_node:
                raise NotFoundError('Program {} not found'.format(program))
            # Check that the project exists
            project_node = (
                flask.current_app
                .db
                .nodes(models.Project)
                .props(code=project)
                .path('programs')
                .ids(program_node.node_id)
                .first()
            )
            if not project_node:
                raise NotFoundError('Project {} not found'.format(project))
        return func(program, project, *args, **kwargs)
    return check_and_call


def check_action_allowed_for_file(indexd_doc, node, action, file_state, s3_url):

    # Checks if we have a valid graph node
    if node:
        # Node is released or submitted
        if node.state in [u'released', u'submitted']:
            raise UserError('Node {} is already {}'.format(node, node.state))
    # Otherwise, we're working off of just the indexd node
    elif indexd_doc:
        if indexd_doc.version or indexd_doc.metadata.get('release_number'):
            raise UserError('Doc {} is already released: ver {}, release {}', 
                indexd_doc.did, indexd_doc.version, indexd_doc.metadata.get('release_number'))

    # if record not found, allow action
    if file_state is None:
        return

    not_allowed_state = (
        action in ['upload', 'initiate_multipart']
        and file_state not in ALLOWED_STATES_FOR_UPLOAD
    )
    not_uploading_state = (
        action in UPLOADING_PARTS and file_state != UPLOADING_STATE
    )
    not_success_state = (
        action == 'get_file' and file_state != UPLOADED_STATE
    )
    if not_allowed_state or not_uploading_state or not_success_state:
        raise UserError(
            'File in {} state, {} not allowed'.format(file_state, action)
        )


def create_entity_list(nodes):
    docs = []
    for node in nodes:
        props = {k: v for k, v in node.props.iteritems()}
        props['id'] = node.node_id
        props['type'] = node.label
        if hasattr(node, 'project_id'):
            program = node.project_id.split('-')[0]
            project = '-'.join(node.project_id.split('-')[1:])
        else:
            program, project = None, None
        for link_name in node._pg_links:  # pylint: disable=W0212
            neighbors = getattr(node, link_name)
            if neighbors:
                props[link_name] = [{
                    'id': neighbor.node_id,
                    'submitter_id': neighbor.props.get('submitter_id', None)
                } for neighbor in neighbors]
        docs.append({
            'program': program,
            'project': project,
            'properties': props,
        })
    return docs


def get_all_template(file_format, categories=None, exclude=None, **kwargs):
    """
    Return template in format `file_format` for given categories.

    ..note: kwargs absorbs `project`, `program` intended for future use
    """
    categories = categories.split(',') if categories else []
    exclude = exclude.split(',') if exclude else []
    entity_types = [
        entity_type
        for entity_type, schema in dictionary.schema.iteritems()
        if 'project_id' in schema.get('properties', {})
        and (not categories or schema['category'] in categories)
        and (not exclude or entity_type not in exclude)
    ]
    if file_format == 'json':
        return get_json_template(entity_types)
    else:
        return get_delimited_template(entity_types, file_format)


def get_delimited_template(entity_types, file_format, filename=TEMPLATE_NAME):
    """
    TODO

    Args:
        entity_types: TODO
        file_format: TODO
        filename: TODO

    Return:
        ``file_format`` (TSV or CSV) template for entity types.
    """
    tar_obj = StringIO.StringIO()
    tar = tarfile.open(filename, mode='w|gz', fileobj=tar_obj)

    for entity_type in entity_types:
        content = entity_to_template_str(entity_type, file_format=file_format)
        partname = '{}.{}'.format(entity_type, file_format)
        tarinfo = tarfile.TarInfo(name=partname)
        tarinfo.size = len(content)
        tar.addfile(tarinfo, StringIO.StringIO(content))

    tar.close()
    return tar_obj.getvalue()


def get_external_proxies():
    """
    Get any custom proxies set in the config.

    This is a rather specific use case, but here we want to reach an external
    resource via a proxy but do not want to set the proxy environment variable
    proper.

    This value should be added to ``app.config['EXTERNAL_PROXIES']``, and
    should look something like

    .. code-block:: JavaScript

        {
            'http': "http://<http_proxy:port>",
            'http': "https://<https_proxy:port>",
        }

    :return:
        A Dictionary ``{'http': ..., 'https': ...}`` with proxies. If a certain
        proxy is not specified, it should be absent from the dictionary.
    """
    return flask.current_app.config.get('EXTERNAL_PROXIES', {})


def get_json_template(entity_types):
    """Return json template for entity types."""
    return json_dumps_formatted([
        entity_to_template(entity_type, file_format='json')
        for entity_type in entity_types
    ])


def get_node(project_id, uuid, db=None):
    if db is None:
        db = flask.current_app.db
    with db.session_scope():
        node = db.nodes().ids(uuid).props(project_id=project_id).first()
    if node:
        return node
    else:
        raise UserError(
            "File {} doesn't exist in {}".format(uuid, project_id)
        )


def get_indexd(uuid, indexd_client, return_not_found=False):
    """Get indexd doc for a given UUID

    Args:
        uuid (string): UUID that is possibly in the system
        return_not_found (bool): If True, will return None if record does not exist
    Returns:
        doc: Indexd doc to be modified later
    """

    indexd_doc = indexd_client.get(uuid)
    if indexd_doc is None and not return_not_found:
        raise InternalError(
            "Indexd entry for {} doesn't exist".format(uuid)
        )
    return indexd_doc


def get_indexd_state(did, url, indexd_client, return_not_found=False):
    """Get file state from indexd urls_metadata
    Args:
        did (string): document id in indexd database
        return_not_found (bool): If True, will return None if record does not exist
    Returns:
        state for the main storage url stored in urls_metadata
    """
    indexd_doc = get_indexd(did, indexd_client, return_not_found=return_not_found)

    if indexd_doc is None:
        return None

    # If url is provided return url's 'state' or None
    if url is not None:
        return indexd_doc.urls_metadata.get(url, {}).get('state')

    # If url is None, lookup primary url
    for url, url_meta in indexd_doc.urls_metadata.items():
        if url_meta.get('type') == PRIMARY_URL_TYPE:
            return url_meta.get('state')

    raise UserError('No primary url found for {}'.format(did))


def set_indexd_state(indexd_doc, url, state):
    """Update url state in indexd

    You have to return the patched version of the indexd Document object
    because it gets modified, if you intend on using the same doc in other
    parts of your program.

    Args:
        indexd_doc (indexclient.client.Document): Representation of indexd doc
        url (str): key of urls_metadata you wish to change
        state (str): state you wish to change it to

    Returns:
        indexclient.client.Document: indexd doc object
    """
    indexd_doc.urls_metadata[url]['state'] = state
    indexd_doc.patch()
    return indexd_doc


def get_suggestion(value, choices):
    """Generate a suggestion to help with typos, etc."""
    message = ""
    try:
        suggestion, score = extract(value, choices, limit=1)[0]
        if score > 70:
            message = " Did you mean '{}'?".format(suggestion)
        else:
            message = ""
    except:  # pylint: disable=bare-except
        pass
    return message


def get_variables(payload):
    """
    TODO
    """
    variables = None
    errors = None
    var_payload = payload.get('variables')
    if isinstance(var_payload, dict):
        variables = var_payload
    else:
        try:
            variables = json.loads(var_payload) if var_payload else {}
        except Exception as e:  # pylint: disable=broad-except
            errors = ['Unable to parse variables', str(e)]
    return variables, errors


def generate_s3_url(host, bucket, program, project, uuid, file_name):
    """
    Determine what the s3 url will be so we can assign file states before a file
    is uploaded

    Example:
        s3://HOST/BUCKET/PROGRAM-PROJECT/UUID/FILENAME

    Args:
        host (str): s3 hostname
        bucket (str): s3 bucket name
        program (str): program name
        project (str): project code
        uuid (str): entity's did
        file_name (str): entity's filename

    Returns:
        str: valid s3 url
    """

    if not host.startswith('s3://'):
        host = 's3://' + host

    if bucket.startswith('/'):
        bucket = bucket[1:]

    return '/'.join(
        [host, bucket, generate_s3_key(program, project, uuid, file_name)])


def generate_s3_key(program, project, uuid, file_name):
    """Generate an object store key given program, project, uuid and file_name

    NOTE: This is an attempt to centralize object store key generation and move
    it outside of `sheepdog.utils.s3.make_s3_request`

    Returns:
        str: Key in form: <program>-<project>/<uuid>/<file_name>
    """
    return '{}-{}/{}/{}'.format(program, project, uuid, file_name)


def is_property_hidden(key, schema, exclude_id):
    """
    Indicate whether key should be hidden.
    """
    is_system_prop = key in schema['systemProperties'] and key not in ['id']
    is_excluded_id = exclude_id and key == 'id'
    return is_system_prop or is_excluded_id


def is_flag_set(flag, default=False):
    """
    Check if the value of a flag is specified (e.g. "?async=true"). Requires
    flask request context.
    """
    value = flask.request.args.get(flag, default)
    if isinstance(value, bool):
        return value
    elif value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    else:
        raise UserError('Boolean value not one of [true, false]')


def json_dumps_formatted(data):
    """Return json string with standard format."""
    dump = json.dumps(
        data, indent=2, separators=(', ', ': '), ensure_ascii=False
    )
    return dump.encode('utf-8')


def jsonify_check_errors(data_and_errors, error_code=400):
    """
    TODO
    """
    data, errors = data_and_errors
    if errors:
        return flask.jsonify({'data': data, 'errors': errors}), error_code
    return flask.jsonify({'data': data}), 200


@contextmanager
def log_duration(name="Unnamed action"):
    """
    Provide context manager for executing code and logging the duration.
    """
    start_t = time.time()
    yield
    end_t = time.time()
    msg = "Executed [{}] in {:.2f} ms".format(name, (end_t-start_t)*1000)
    flask.current_app.logger.info(msg)


def lookup_project(db_driver, program_name, project_code):
    """
    Lookup project node in database.

    Args:
        program_name: Project.programs[0].code
        project_code: Project.name

    Return:
        models.Project: None if no project found, else the matching
        :class:`models.Project`.
    """
    with db_driver.session_scope():
        return (
            db_driver.nodes(models.Project)
            .props(code=project_code)
            .path('programs')
            .props(name=program_name)
            .scalar()
        )


def lookup_program(psql_driver, program):
    """Return a program by Program.name"""
    return psql_driver.nodes(models.Program).props(name=program).scalar()


def proxy_request(project_id, uuid, data, args, headers, method, action,
                  indexd_client, dry_run=False):
    """Proxy a request to an object storage"""

    node = get_node(project_id, uuid)
    indexd_doc = indexd_client.get(uuid)
    s3_url = None
    key_name = None
    for url, metadata in indexd_doc.urls_metadata.items():
        if metadata.get('type') == PRIMARY_URL_TYPE:
            s3_url = url
            path = urlparse(url).path
            # /one/two/three -> two/three
            key_name = '/'.join([p for p in path.split('/') if p][1:])
            break

    if not s3_url:
        InternalError('URL does not exist for {}'.format(uuid))

    file_state = indexd_doc.urls_metadata[s3_url].get('state')

    check_action_allowed_for_file(indexd_doc, node, action, file_state, s3_url)

    if dry_run:
        message = (
            'Transaction would have been successful. User selected dry run'
            ' option; transaction aborted, no data written to object storage.'
        )
        return flask.Response(json.dumps({'message': message}), status=200)

    if action in ['upload', 'initiate_multipart']:
        set_indexd_state(indexd_doc, s3_url, UPLOADING_STATE)

    elif action == 'abort_multipart':
        set_indexd_state(indexd_doc, s3_url, submitted_state())

    if action not in ['upload', 'upload_part', 'complete_multipart']:
        data = ''

    resp = s3.make_s3_request(key_name, data, args, headers, method, action)

    if action in ['upload', 'complete_multipart'] and resp.status == 200:
        set_indexd_state(indexd_doc, s3_url, UPLOADED_STATE)

    elif action == 'delete' and resp.status == 204:
        set_indexd_state(indexd_doc, s3_url, DELETE_STATE)

    return resp


def is_node_file(node):
    """Returns True if the object is a file (i.e. it may have
    corresponding data in the object store)
    """

    return node._dictionary['category'] in DATA_FILE_CATEGORIES


def is_project_public(project):
    if not hasattr(models.Project, 'availability_type'):
        return False
    return project.availability_type == 'Open'


def should_send_email(config):
    """Only opt to send an email if the following are provided

    You must set these variables in your flask curren_app's config
    """

    required_email_fields = [
        'EMAIL_FROM_ADDRESS',               # from
        'EMAIL_SUPPORT_ADDRESS',            # to
        'EMAIL_NOTIFICATION_SUBMISSION',    # email body
    ]

    for field in required_email_fields:
        if field not in config:
            return False
    return True


def assert_dry_run_transaction(program, project, tx_log):
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
