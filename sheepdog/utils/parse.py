"""
TODO
"""

from collections import Counter
import simplejson
import yaml

import flask

from sheepdog.errors import UserError


def oph_raise_for_duplicates(object_pairs):
    """
    Given an list of ordered pairs, contstruct a dict as with the normal JSON
    ``object_pairs_hook``, but raise an exception if there are duplicate keys
    with a message describing all violations.
    """
    counter = Counter(p[0] for p in object_pairs)
    duplicates = [p for p in counter.items() if p[1] > 1]
    if duplicates:
        raise ValueError(
            "The document contains duplicate keys: {}".format(
                ",".join(d[0] for d in duplicates)
            )
        )
    return {pair[0]: pair[1] for pair in object_pairs}


def parse_json(raw):
    """
    Return a python representation of a JSON document.

    Args:
        raw (str): string of raw JSON content

    Raises:
        UserError: if any exception is raised parsing the JSON body

    .. note:: Uses :func:`oph_raise_for_duplicates` in parser.
    """
    try:
        return simplejson.loads(raw, object_pairs_hook=oph_raise_for_duplicates)
    except Exception as e:
        raise UserError("Unable to parse json: {}".format(e))


def parse_request_json(expected_types=(dict, list)):
    """
    Return a python representation of a JSON POST body.

    Args:
        raw (str): string of raw JSON content

    Return:
        TODO

    Raises:
        UserError: if any exception is raised parsing the JSON body
        UserError: if the result is not of the expected type

    If raw is not provided, pull the body from global request object.
    """
    parsed = parse_json(flask.request.get_data())
    if not isinstance(parsed, expected_types):
        raise UserError(
            "JSON parsed from request is an invalid type: {}".format(
                parsed.__class__.__name__
            )
        )
    return parsed


def parse_request_yaml():
    """
    Return a python representation of a YAML POST body. Raise UserError if any
    exception is raised parsing the YAML body.
    """
    try:
        return yaml.safe_load(flask.request.get_data())
    except Exception as e:
        raise UserError("Unable to parse yaml: {}".format(e))
