# pylint: disable=unsubscriptable-object
"""
Provide utility functions primarily for code in ``sheepdog.blueprint``
(though some are also used in ``sheepdog.upload``).
"""

from contextlib import contextmanager
import copy
import csv
import functools
import html
import json
import os
import io
import tarfile
import time

import boto
import flask
from fuzzywuzzy.process import extract
import psqlgraph

from sheepdog import dictionary
from sheepdog import models
from sheepdog.errors import InternalError, NotFoundError, UnsupportedError, UserError
from sheepdog.globals import (
    submitted_state,
    DELIMITERS,
    TEMPLATE_NAME,
    UPLOADING_STATE,
    SUCCESS_STATE,
    ERROR_STATE,
    UPLOADING_PARTS,
)
from sheepdog.utils.transforms.graph_to_doc import (
    entity_to_template,
    entity_to_template_delimited,
    entity_to_template_json,
    entity_to_template_str,
    get_node_category,
)
from . import parse
from . import s3
from . import scheduling


ALLOWED_STATES = [ERROR_STATE, submitted_state(), UPLOADING_STATE]


def _get_links(file_format, schema, exclude_id):
    """
    Parse links from schema.

    we don't have project specific schema now
    so right now this uses top level schema

    TODO
    """
    links = dict()
    subgroups = [link for link in schema if "subgroup" in link]
    non_subgroups = [link for link in schema if "name" in link]
    for link in non_subgroups:
        if file_format == "json":
            links[link["name"]] = _get_links_json(link, exclude_id)
        else:
            links[link["name"]] = _get_links_delimited(link, exclude_id)
    for subgroup in subgroups:
        links.update(_get_links(file_format, subgroup["subgroup"], exclude_id))
    return links


def _get_links_json(link, exclude_id):
    """
    Return parsed link template from link schema in json form.
    """
    target_schema = dictionary.schema[link["target_type"]]
    link_template = dict(
        {k: None for subkeys in target_schema.get("uniqueKeys", []) for k in subkeys}
    )
    if "project_id" in link_template:
        del link_template["project_id"]
    if exclude_id:
        del link_template["id"]
    return link_template


def _get_links_delimited(link, exclude_id):
    """
    Return parsed link template from link schema in delimited form.
    """
    link_template = []
    target_schema = dictionary.schema[link["target_type"]]
    # default key for link is the GDC ID
    if not exclude_id:
        link_template.append("id")

    unique_keys = [key for key in target_schema["uniqueKeys"] if key != ["id"]]

    for unique_key in unique_keys:
        keys = copy.copy(unique_key)
        if "project_id" in keys:
            keys.remove("project_id")
        link_template += [prop for prop in keys]

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
            programs = flask.current_app.db.nodes(models.Program).props(name=program)
            if not programs.count():
                raise NotFoundError("program {} not found".format(program))
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
                flask.current_app.db.nodes(models.Program).props(name=program).first()
            )
            if not program_node:
                raise NotFoundError("Program {} not found".format(program))
            # Check that the project exists
            project_node = (
                flask.current_app.db.nodes(models.Project)
                .props(code=project)
                .path("programs")
                .ids(program_node.node_id)
                .first()
            )
            if not project_node:
                raise NotFoundError("Project {} not found".format(project))
            phsids = [
                program_node.dbgap_accession_number,
                project_node.dbgap_accession_number,
            ]
        return func(program, project, *args, **kwargs)

    return check_and_call


def check_action_allowed_in_state(action, file_state):
    not_allowed_state = (
        action in ["upload", "initiate_multipart"] and file_state not in ALLOWED_STATES
    )
    not_uploading_state = action in UPLOADING_PARTS and file_state != UPLOADING_STATE
    not_success_state = action == "get_file" and file_state != SUCCESS_STATE
    if not_allowed_state or not_uploading_state or not_success_state:
        raise UserError("File in {} state, {} not allowed".format(file_state, action))


def create_entity_list(nodes):
    docs = []
    for node in nodes:
        props = {k: v for k, v in node.props.items()}
        props["id"] = node.node_id
        props["type"] = node.label
        if hasattr(node, "project_id"):
            program, project = node.project_id.split("-", 1)
        else:
            program, project = None, None
        for link_name in node._pg_links:  # pylint: disable=W0212
            neighbors = getattr(node, link_name)
            if neighbors:
                props[link_name] = [
                    {
                        "id": neighbor.node_id,
                        "submitter_id": neighbor.props.get("submitter_id", None),
                    }
                    for neighbor in neighbors
                ]
        docs.append({"program": program, "project": project, "properties": props})
    return docs


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
    return flask.current_app.config.get("EXTERNAL_PROXIES", {})


def get_json_template(entity_types):
    """Return json template for entity types."""
    return json_dumps_formatted(
        [
            entity_to_template(entity_type, file_format="json")
            for entity_type in entity_types
        ]
    )


def get_node(project_id, uuid, db=None):
    if db is None:
        db = flask.current_app.db
    with db.session_scope():
        node = db.nodes().ids(uuid).props(project_id=project_id).first()
    if node:
        return node
    else:
        raise UserError("File {} doesn't exist in {}".format(uuid, project_id))


def get_file_record(uuid):
    """Get file record for a given UUID

    Args:
        uuid (string): UUID that is possibly in the system
        passive (bool): if a uuid doesn't exist, that's ok
    Returns:
        file_record: file record to be modified later
    """

    file_record = flask.current_app.index_client.get(uuid)
    if file_record is None:
        raise InternalError("File record for {} doesn't exist".format(uuid))
    return file_record


def get_suggestion(value, choices):
    """Generate a suggestion to help with typos, etc."""
    message = ""
    try:
        suggestion, score = extract(value, choices, limit=1)[0]
        if score > 70:
            message = " Did you mean '{}'?".format(suggestion)
        else:
            message = ""
    except Exception:
        pass
    return message


def get_variables(payload):
    """
    TODO
    """
    variables = None
    errors = None
    var_payload = payload.get("variables")
    if isinstance(var_payload, dict):
        variables = var_payload
    else:
        try:
            variables = json.loads(var_payload) if var_payload else {}
        except Exception as e:  # pylint: disable=broad-except
            errors = ["Unable to parse variables", str(e)]
    return variables, errors


def is_property_hidden(key, schema, exclude_id):
    """
    Indicate whether key should be hidden.
    """
    is_system_prop = key in schema["systemProperties"] and key not in ["id"]
    is_excluded_id = exclude_id and key == "id"
    return is_system_prop or is_excluded_id


def is_flag_set(flag, default=False):
    """
    Check if the value of a flag is specified (e.g. "?async=true"). Requires
    flask request context.
    """
    value = flask.request.args.get(flag, default)
    if isinstance(value, bool):
        return value
    elif value.lower() == "true":
        return True
    elif value.lower() == "false":
        return False
    else:
        raise UserError("Boolean value not one of [true, false]")


def json_dumps_formatted(data):
    """Return json string with standard format."""
    dump = json.dumps(data, indent=2, separators=(", ", ": "), ensure_ascii=False)
    return dump.encode("utf-8")


def jsonify_check_errors(data_and_errors, error_code=400):
    """
    TODO
    """
    data, errors = data_and_errors
    if errors:
        return flask.jsonify({"data": data, "errors": errors}), error_code
    return flask.jsonify({"data": data}), 200


@contextmanager
def log_duration(name="Unnamed action"):
    """
    Provide context manager for executing code and logging the duration.
    """
    start_t = time.time()
    yield
    end_t = time.time()
    msg = "Executed [{}] in {:.2f} ms".format(name, (end_t - start_t) * 1000)
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
            .path("programs")
            .props(name=program_name)
            .scalar()
        )


def lookup_program(psql_driver, program):
    """Return a program by Program.name"""
    return psql_driver.nodes(models.Program).props(name=program).scalar()


def proxy_request(project_id, uuid, data, args, headers, method, action, dry_run=False):
    node = get_node(project_id, uuid)
    check_action_allowed_in_state(action, node.file_state)
    file_record = get_file_record(uuid)

    if dry_run:
        message = (
            "Transaction would have been successful. User selected dry run"
            " option; transaction aborted, no data written to object storage."
        )
        return flask.Response(json.dumps({"message": message}), status=200)

    if action in ["upload", "initiate_multipart"]:
        update_state(node, UPLOADING_STATE)
    elif action == "abort_multipart":
        update_state(node, submitted_state())

    if action not in ["upload", "upload_part", "complete_multipart", "reassign"]:
        data = ""

    if action == "reassign":
        try:
            # data.read() works like a file pointer.
            # When you .read() again it will be pointing at the end of the stream
            json_data = data.read()

            # if it comes in as a string, convert it to dict
            if not isinstance(json_data, dict):
                json_data = json.loads(json_data)

            new_url = json_data["s3_url"]

        except Exception:
            message = "Unable to parse json. Use the format {'s3_url':'s3/://...'}"
            return flask.Response(json.dumps({"message": message}), status=400)

        update_file_record_url(file_record, s3_url=new_url)
        update_state(node, SUCCESS_STATE)
        message = "URL successfully reassigned. New url: {}".format(
            html.escape(new_url)
        )
        return flask.Response(json.dumps({"message": message}), status=200)

    resp = s3.make_s3_request(project_id, uuid, data, args, headers, method, action)
    if action in ["upload", "complete_multipart"]:
        if resp.status == 200:
            update_file_record_url(file_record, project_id + "/" + uuid)
            update_state(node, SUCCESS_STATE)
    if action == "delete":
        if resp.status == 204:
            update_state(node, submitted_state())
            update_file_record_url(file_record, None)
    return resp


def update_state(node, state):
    with flask.current_app.db.session_scope() as s:
        s.add(node)
        node.file_state = state


def update_file_record_url(file_record, key_name=None, s3_url=None):
    """Update a file record with a new URL.

    Args:
        file_record: File record that will be modified
            with a new URL.
        key_name (string): Name of the s3 key to update a file record with.
        s3_url (string): The URL you wish assign a file record with.
    """

    if key_name:
        url = "s3://{host}/{bucket}/{name}".format(
            host=flask.current_app.config["SUBMISSION"]["host"],
            bucket=flask.current_app.config["SUBMISSION"]["bucket"],
            name=key_name,
        )
        file_record.urls = [url]
    elif s3_url:
        file_record.urls = [s3_url]
    else:
        file_record.urls = []
    file_record.patch()


def is_node_file(node):
    """Returns True if the object is a file (i.e. it may have
    corresponding data in the object store)
    """

    return node._dictionary["category"].endswith("_file")


def is_project_public(project):
    if not hasattr(models.Project, "availability_type"):
        return False
    return project.availability_type == "Open"


def should_send_email(config):
    """Only opt to send an email if the following are provided

    You must set these variables in your flask curren_app's config
    """

    required_email_fields = [
        "EMAIL_FROM_ADDRESS",  # from
        "EMAIL_SUPPORT_ADDRESS",  # to
        "EMAIL_NOTIFICATION_SUBMISSION",  # email body
    ]

    for field in required_email_fields:
        if field not in config:
            return False
    return True
