import pytest

from peregrine import esutils
from peregrine.errors import UserError


@pytest.mark.parametrize("params,expected", [
    # filters is a dict
    ({"filters": {
        "op": "=",
        "content": {
            "field": "project_code",
            "value": ["ACC"]
        }}},
     {'op': '=',
      'content': {
          'field': 'project_code',
          'value': ['ACC']}
      }),
    # filters is a string
    ({"filters": """{"op": "=",
      "content": {
          "field": "project_code",
          "value": ["ACC"]
          }}"""},
     {'content': {'field': 'project_code', 'value': ['ACC']}, 'op': '='}
     )
])
def test_get_filters(params, expected):
    assert esutils.search.get_filters(params) == expected


def test_get_filters_bad_json():
    with pytest.raises(UserError):
        esutils.search.get_filters({"filters": "asdf adsf : adf "})


@pytest.mark.parametrize("facets,expected", [
    (["myfacet"], {
        'aggs': {
            'myfacet_global': {
                'aggs': {
                    'myfacet_filtered': {
                        'aggs': {
                            'myfacet': {'terms': {'field': 'myfacet', 'size': 100}},
                            'myfacet_missing': {'missing': {'field': 'myfacet'}}
                        },
                        'filter': {
                            'match_all': {}
                        }
                    }
                },
                'global': {}
            }
        }
    })
])
def test_build_body(facets, expected):
    assert esutils.search.build_body("doc_type", [], {"facets": facets}) == expected


def test_build_body_without_filters():
    facets = ['project_code']
    expected = {'aggs': esutils.search.build_aggregations(facets, [], {}, "doc_type")}
    assert esutils.search.build_body("doc_type", [], {"facets": facets}) == expected


def test_build_body_without_facets():
    params = {
        "filters": {
            "op": "!=",
            "content": {
                "field": "project_code",
                "value": "ACC"
            }
        }
    }
    expected = {
        "query": {
            "filtered": {
                "query": {
                    "match_all": {}
                },
                "filter": esutils.request.build_filters("doc_type", [], params["filters"])
            }
        }
    }
    assert esutils.search.build_body("doc_type", [], params) == expected


@pytest.mark.parametrize("hits,params,expected", [
    # 0 - empty hits, from 0
    ({"hits": [], "total": 0}, {"size": 10, "from": 0, "sort": "foo"}, {
        "count": 0,
        "total": 0,
        "size": 10,
        "from": 1,
        "sort": "foo",
        "page": 0,
        "pages": 0
    }),
    # 1- empty hits, from 1
    ({"hits": [], "total": 0}, {"size": 10, "from": 1, "sort": "foo"}, {
        "count": 0,
        "total": 0,
        "size": 10,
        "from": 2,
        "sort": "foo",
        "page": 0,
        "pages": 0
    }),
    # 2 - 1 hit 1 total
    ({"hits": [1], "total": 1}, {"size": 10, "from": 0, "sort": "foo"}, {
        "count": 1,
        "total": 1,
        "size": 10,
        "from": 1,
        "sort": "foo",
        "page": 1,
        "pages": 1
    }),
    # 3 - 1 hit 2 total size 10
    ({"hits": [1], "total": 2}, {"size": 10, "from": 0, "sort": "foo"}, {
        "count": 1,
        "total": 2,
        "size": 10,
        "from": 1,
        "sort": "foo",
        "page": 1,
        "pages": 1
    }),
    # 4 - 1 hit 4 total size 1
    ({"hits": [1], "total": 4}, {"size": 1, "from": 0, "sort": "foo"}, {
        "count": 1,
        "total": 4,
        "size": 1,
        "from": 1,
        "sort": "foo",
        "page": 1,
        "pages": 4
    }),
    # 5 - 10 hit 100 total size 10
    ({"hits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "total": 100}, {"size": 10, "from": 0, "sort": "foo"}, {
        "count": 10,
        "total": 100,
        "size": 10,
        "from": 1,
        "sort": "foo",
        "page": 1,
        "pages": 10
    }),
    # 6 - 5 hit 100 total size 10 from 90
    ({"hits": [91, 92, 93, 94, 95, 96, 97, 98, 99, 100], "total": 100}, {"size": 10, "from": 90, "sort": "foo"}, {
        "count": 10,
        "total": 100,
        "size": 10,
        "from": 91,
        "sort": "foo",
        "page": 10,
        "pages": 10
    }),
    # 7 - 5 hit 100 total size 10 from 95
    ({"hits": [96, 97, 98, 99, 100], "total": 100}, {"size": 10, "from": 95, "sort": "foo"}, {
        "count": 5,
        "total": 100,
        "size": 10,
        "from": 96,
        "sort": "foo",
        "page": 10,
        "pages": 10
    }),
])
def test_build_pagination(hits, params, expected):
    assert esutils.search.build_pagination(hits, params) == expected
