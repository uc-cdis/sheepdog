import pytest

from peregrine import utils
from peregrine.models import file

@pytest.mark.parametrize("defaults,params,expected", [
    ({}, {"a": 1}, {"a": 1}),
    ({"a": 1}, {}, {"a": 1}),
    ({"a": 1}, {"b": 10}, {"a": 1, "b": 10}),
    ({"a": 1, "b": {"c": 2}}, {"b": {"d": 4}, "e": 5}, {"a": 1, "b": {"c": 2, "d": 4}, "e": 5}),
    ({"a": 1, "b": [{"c": 2}, {"c": 3}]}, {"b": [{"d": 4}, {"d": 5}], "e": 5},
     {"a": 1, "b": [{"c": 2, "d": 4}, {"c": 3, "d": 5}], "e": 5})
])
def test_merge(defaults, params, expected):
    assert utils.merge(defaults, params) == expected


@pytest.mark.parametrize("defaults,params,expected", [
    ({}, {"a": 1}, {}),
    ({"a": 1}, {}, {"a": 1}),
    ({"a": 1, "b": 2}, {"b": 20, "c": 3}, {"a": 1, "b": 20}),
    ({"a": 1}, {"a": 10}, {"a": 10})
])
def test_merge_into_defaults(defaults, params, expected):
    assert utils.merge_into_defaults(defaults, params) == expected


def text_validate_sorting_invalid_doc_type():
    params = {
        'sort': 'a,,b,,c',
    }

    doc_type = 'foo'

    assert utils.validate_sorting(params, doc_type) == params


def test_validate_sorting_does_not_rewrite():
    """ Validating sorting does not rewrite non-MULTI_MATCH_FIELDS orderings.
    """
    params = {
        'sort': 'a,,b,,c',
    }

    doc_type = 'file'

    assert utils.validate_sorting(params, doc_type) == params


def test_validate_sorting_substitutes_raw():
    """ Validating sorting substitutes MULTI_MATCH_FIELDS for raw variants.
    """
    fields = sorted(file.MULTI_MATCH_FIELDS)
    raw_fields = map(lambda x: x+'.raw', fields)

    params = {
        'sort': ','.join(fields),
    }

    expect = {
        'sort': ','.join(raw_fields),
    }

    doc_type = 'file'

    assert utils.validate_sorting(params, doc_type) == expect


def test_validate_sorting_maintains_direction():
    """ Validating sorting does not rewrite sort direction.
    """
    params = {
        'sort': 'a:desc,,b:asc,,c:desc',
    }

    doc_type = 'file'

    assert utils.validate_sorting(params, doc_type) == params


def test_validate_sorting_substitues_raw_maintains_direction():
    """ validate_sorting does not rewrite sort direction w/ raw variants.
    """
    fields = [x+':desc' for x in sorted(file.MULTI_MATCH_FIELDS)]
    raw_fields = map(lambda x: x.replace(':desc', '.raw:desc'), fields)

    params = {
        'sort': ','.join(fields),
    }

    expect = {
        'sort': ','.join(raw_fields),
    }

    doc_type = 'file'

    assert utils.validate_sorting(params, doc_type) == expect
