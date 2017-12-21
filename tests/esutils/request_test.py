import pytest

from peregrine.esutils import request


@pytest.mark.parametrize("params,expected", [
    ({}, ({}, {})),
    ({"fields": "foo"}, ({"fields": "foo"}, {})),
    ({"fields": "foo,bar"}, ({"fields": "foo,bar"}, {})),
    ({"fields": "baz"}, ({"fields": ""}, {"fields": 'unrecognized values: [baz]'})),
    ({"fields": "foo,bar,baz"}, ({"fields": "foo,bar"}, {"fields": 'unrecognized values: [baz]'})),
])
def test_sanitize(params, expected):
    allowed = {
        "fields": ["foo", "bar"],
        "facets": ["foo"],
        "expand": ["foo"]
    }
    assert request.sanitize(params, allowed) == expected


@pytest.mark.parametrize("key,parent,expected", [
    ("bcr_case_barcode", None,
     {'bcr_case_barcode_missing': {'missing': {'field': 'bcr_case_barcode'}},
      'bcr_case_barcode': {'terms': {'field': 'bcr_case_barcode', 'size': 100}}}),
    ("is_ffpe", "samples",
     {'is_ffpe_missing': {'aggs': {'is_ffpe_missing_rn': {'reverse_nested': {}}},
                          'missing': {'field': 'samples.is_ffpe'}},
      'is_ffpe': {'terms': {'field': 'samples.is_ffpe', 'size': 100},
                  'aggs': {'is_ffpe_rn': {'reverse_nested': {}}}}})
])
def test_get_field_value(key, parent, expected):
    assert request._aggs_get_field_value(key, [], parent=parent) == expected


@pytest.mark.parametrize("ks,expected", [
    # 0 /cases?facets=race
    ("field1",
     {'field1_global': {'global': {}, 'aggs': {'field1_filtered': {'filter': {'match_all': {}},
                                                                   'aggs': {'field1': {
                                                                       'terms': {'field': 'field1', 'size': 100}},
                                                                       'field1_missing': {
                                                                           'missing': {'field': 'field1'}}}}}}}),
    # 1 /cases?facets=samples.is_ffpe
    ("nested1.field1",
     {'nested1.field1_global': {'global': {},
                                'aggs': {'nested1.field1_filtered': {'filter': {'match_all': {}},
                                                                     'aggs': {'nested1': {'aggs': {'field1': {
                                                                         'terms': {'field': 'nested1.field1',
                                                                                   'size': 100},
                                                                         'aggs': {'field1_rn': {'reverse_nested': {}}}},
                                                                         'field1_missing': {
                                                                             'aggs': {
                                                                                 'field1_missing_rn': {
                                                                                     'reverse_nested': {}}},
                                                                             'missing': {
                                                                                 'field': 'nested1.field1'}}},
                                                                         'nested': {
                                                                             'path': 'nested1'}}}}}}}
     ),
    # 2 /projects?facets=summary._experimental_data.experimental_type
    ("simple1.nested1.field1",
     {'simple1.nested1.field1_global': {'global': {},
                                        'aggs': {'simple1.nested1.field1_filtered': {'filter': {'match_all': {}},
                                                                                     'aggs': {'simple1.nested1': {
                                                                                         'aggs': {'field1': {'terms': {
                                                                                             'field': 'simple1.nested1.field1',
                                                                                             'size': 100},
                                                                                             'aggs': {
                                                                                                 'field1_rn': {
                                                                                                     'reverse_nested': {}}}},
                                                                                             'field1_missing': {
                                                                                                 'aggs': {
                                                                                                     'field1_missing_rn': {
                                                                                                         'reverse_nested': {}}},
                                                                                                 'missing': {
                                                                                                     'field': 'simple1.nested1.field1'}}},
                                                                                         'nested': {
                                                                                             'path': 'simple1.nested1'}}}}}}}
     ),
    # 3 /cases?facets=files.archive.disease_code
    ("nested1.simple1.field1",
     {'nested1.simple1.field1_global': {'global': {},
                                        'aggs': {'nested1.simple1.field1_filtered': {'filter': {'match_all': {}},
                                                                                     'aggs': {'nested1': {'aggs': {
                                                                                         'simple1.field1': {'terms': {
                                                                                             'field': 'nested1.simple1.field1',
                                                                                             'size': 100}, 'aggs': {
                                                                                             'simple1.field1_rn': {
                                                                                                 'reverse_nested': {}}}},
                                                                                         'simple1.field1_missing': {
                                                                                             'aggs': {
                                                                                                 'simple1.field1_missing_rn': {
                                                                                                     'reverse_nested': {}}},
                                                                                             'missing': {
                                                                                                 'field': 'nested1.simple1.field1'}}},
                                                                                         'nested': {
                                                                                             'path': 'nested1'}}}}}}}
     ),
    # 4 /cases?facets=samples.portions.is_ffpe
    ("nested1.nested2.field1",
     {'nested1.nested2.field1_global': {'global': {}, 'aggs': {
         'nested1.nested2.field1_filtered': {'filter': {'match_all': {}},
                                             'aggs': {'nested1': {'aggs': {'nested2': {'aggs': {'field1': {'terms': {
                                                 'field': 'nested1.nested2.field1', 'size': 100},
                                                 'aggs': {'field1_rn': {'reverse_nested': {}}}},
                                                 'field1_missing': {
                                                     'aggs': {'field1_missing_rn': {'reverse_nested': {}}},
                                                     'missing': {'field': 'nested1.nested2.field1'}}},
                                                 'nested': {'path': 'nested1.nested2'}}},
                                                 'nested': {'path': 'nested1'}}}}}}}
     )
])
def test_build_aggregations(ks, expected):
    nested = ["nested1", "simple1.nested1", "nested2", "nested1.nested2"]
    assert request.build_aggregations(nested, False, "file", {}, ks, parent=None) == expected


@pytest.mark.parametrize("facets,expected", [
    # 0 long fields use stats agg
    ('diagnoses.age_at_diagnosis',
     {'diagnoses.age_at_diagnosis_global': {'aggs': {'diagnoses.age_at_diagnosis_filtered': {
         'aggs': {'diagnoses.age_at_diagnosis_stats': {'stats': {'field': 'diagnoses.age_at_diagnosis'}}},
         'filter': {'match_all': {}}}}, 'global': {}}}),
    ('diagnoses.days_to_death',
     {'diagnoses.days_to_death_global': {'aggs': {'diagnoses.days_to_death_filtered': {
         'aggs': {'diagnoses.days_to_death_stats': {'stats': {'field': 'diagnoses.days_to_death'}}},
         'filter': {'match_all': {}}}}, 'global': {}}}),
    # 1 everything else uses terms
    ('field1',
     {'field1_global': {'aggs': {'field1_filtered': {'aggs': {'field1': {'terms': {'field': 'field1', 'size': 100}},
                                                              'field1_missing': {'missing': {'field': 'field1'}}},
                                                     'filter': {'match_all': {}}}}, 'global': {}}}
     )])
def test_build_aggregations_stats(facets, expected):
    assert request.build_aggregations([], False, "case", {}, facets, parent=None) == expected


@pytest.mark.parametrize("filters,expected", [
    # 0 Range self filters
    ({'content': [{'content': {'field': 'field1', 'value': [10950]}, 'op': '>='},
                  {'content': {'field': 'field1', 'value': [18250]}, 'op': '<='}], 'op': 'and'},
     {'field1_global': {'global': {}, 'aggs': {'field1_filtered': {
         'filter': {'bool': {'must': [{'range': {'field1': {'gte': 10950}}}, {'range': {'field1': {'lte': 18250}}}]}},
         'aggs': {'field1': {'terms': {'field': 'field1', 'size': 100}},
                  'field1_missing': {'missing': {'field': 'field1'}}}}}}}),
    # 1 Non-range filters don't self filter
    ({'content': [{'content': {'field': 'field1', 'value': ['blah']}, 'op': 'in'}], 'op': 'and'},
     {'field1_global': {'global': {}, 'aggs': {'field1_filtered': {'filter': {'match_all': {}}, 'aggs': {
         'field1': {'terms': {'field': 'field1', 'size': 100}}, 'field1_missing': {'missing': {'field': 'field1'}}}}}}})
])
def test_build_aggregations_range(filters, expected):
    assert request.build_aggregations([], filters, "file", {}, "field1", parent=None) == expected


@pytest.mark.parametrize("filters,expected", [
    # 0 - project_code = ACC
    ({"op": "=",
      "content": {
          "field": "project_code",
          "value": ["ACC"]
      }},
     {"terms": {"project_code": ["ACC"]}}),
    # 1 - project_code != ACC
    ({"op": "!=",
      "content": {
          "field": "project_code",
          "value": "ACC"
      }},
     {"bool": {"must_not": [{"terms": {"project_code": ["ACC"]}}]}}),
    # 2 - program = TCGA and status = legacy
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {
               "field": "program",
               "value": ["TCGA"]
           }},
          {"op": "=",
           "content": {
               "field": "status",
               "value": ["legacy"]
           }}]},
     {"bool": {
         "must": [
             {"terms": {"program": ["TCGA"]}},
             {"terms": {"status": ["legacy"]}}]}}),
    # 3 - program = TCGA and status != legacy
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {
               "field": "program",
               "value": ["TCGA"]
           }},
          {"op": "!=",
           "content": {
               "field": "status",
               "value": ["legacy"]
           }}]},
     {"bool": {
         "must": [{"terms": {"program": ["TCGA"]}}],
         "must_not": [{"terms": {"status": ["legacy"]}}]}}),
    # 4 - program = TCGA and project = ACC and status = legacy
    ({"op": "and",
      "content": [
          {"op": "and",
           "content": [
               {"op": "=",
                "content": {
                    "field": "program",
                    "value": ["TCGA"]
                }},
               {"op": "=",
                "content": {
                    "field": "project",
                    "value": ["ACC"]
                }}]},
          {"op": "=",
           "content": {
               "field": "status",
               "value": ["legacy"]
           }}
      ]},
     {"bool": {
         "must": [
             {"terms": {"status": ["legacy"]}},
             {"terms": {"program": ["TCGA"]}},
             {"terms": {"project": ["ACC"]}}]}}),
    # 5 - program = TCGA and project = ACC and status != legacy
    ({"op": "and",
      "content": [
          {"op": "and",
           "content": [
               {"op": "=",
                "content": {
                    "field": "program",
                    "value": ["TCGA"]
                }},
               {"op": "=",
                "content": {
                    "field": "project",
                    "value": ["ACC"]
                }}]},
          {"op": "!=",
           "content": {
               "field": "status",
               "value": ["legacy"]
           }}
      ]},
     {"bool": {
         "must": [
             {"terms": {"program": ["TCGA"]}},
             {"terms": {"project": ["ACC"]}}],
         "must_not": [{"terms": {"status": ["legacy"]}}]}}),
    # 6 - program = TCGA and project != ACC and status = legacy
    ({"op": "and",
      "content": [
          {"op": "and",
           "content": [
               {"op": "=",
                "content": {
                    "field": "program",
                    "value": ["TCGA"]
                }},
               {"op": "!=",
                "content": {
                    "field": "project",
                    "value": ["ACC"]
                }}]},
          {"op": "=",
           "content": {
               "field": "status",
               "value": ["legacy"]
           }}
      ]},
     {"bool": {
         "must": [
             {"terms": {"status": ["legacy"]}},
             {"terms": {"program": ["TCGA"]}}],
         "must_not": [{"terms": {"project": ["ACC"]}}]}}),
    # 7 - program = TCGA and project != ACC and status != legacy
    ({"op": "and",
      "content": [
          {"op": "and",
           "content": [
               {"op": "=",
                "content": {
                    "field": "program",
                    "value": ["TCGA"]
                }},
               {"op": "!=",
                "content": {
                    "field": "project",
                    "value": ["ACC"]
                }}
           ]},
          {"op": "!=",
           "content": {
               "field": "status",
               "value": ["legacy"]
           }}
      ]},
     {"bool": {
         "must": [{"terms": {"program": ["TCGA"]}}],
         "must_not": [
             {"terms": {"status": ["legacy"]}},
             {"terms": {"project": ["ACC"]}}]}}),
    # 8 - program = TCGA or status = legacy
    ({"op": "or",
      "content": [
          {"op": "=",
           "content": {
               "field": "program",
               "value": ["TCGA"]
           }},
          {"op": "=",
           "content": {
               "field": "status",
               "value": ["legacy"]
           }}
      ]},
     {"bool": {
         "should": [
             {"terms": {"program": ["TCGA"]}},
             {"terms": {"status": ["legacy"]}}]}}),
    # 9 - program = TCGA or status != legacy program:TCGA+status!:legacy
    ({"op": "or",
      "content": [
          {"op": "=",
           "content": {
               "field": "program",
               "value": ["TCGA"]
           }},
          {"op": "!=",
           "content": {
               "field": "status",
               "value": ["legacy"]
           }}
      ]},
     {"bool": {
         "should": [
             {"terms": {"program": ["TCGA"]}},
             {"bool": {"must_not": [{"terms": {"status": ["legacy"]}}]}}]}}),
    # 10 - project = ACC or program = TCGA and status = legacy
    ({"op": "or",
      "content": [
          {"op": "=",
           "content": {
               "field": "project",
               "value": ["ACC"]
           }},
          {"op": "and", "content": [
              {"op": "=",
               "content": {
                   "field": "program",
                   "value": ["TCGA"]
               }},
              {"op": "=",
               "content": {
                   "field": "status",
                   "value": ["legacy"]
               }}
          ]}]},
     {"bool": {
         "should": [
             {"terms": {"project": ["ACC"]}},
             {"bool": {"must": [
                 {"terms": {"program": ["TCGA"]}},
                 {"terms": {"status": ["legacy"]}}]}}]}}),
    # 11 - cases.clinical.gender is missing
    ({"op": "is",
      "content": {
          "field": "cases.clinical.gender",
          "value": "missing"
      }},
     {"missing": {"field": "cases.clinical.gender"}}),
    # 12 - cases.clinical.gender not missing
    ({"op": "not",
      "content": {
          "field": "cases.clinical.gender",
          "value": "missing"
      }},
     {"bool": {"must_not": [{"missing": {"field": "cases.clinical.gender"}}]}}),
    # 13 - cases.clinical.gender not missing and cases.clinical.gender is missing
    ({"op": "and",
      "content": [
          {"op": "is",
           "content": {
               "field": "cases.clinical.gender",
               "value": "missing"
           }},
          {"op": "not",
           "content": {
               "field": "cases.clinical.gender",
               "value": "missing"
           }}]},
     {"bool": {"must": [
         {"missing": {"field": "cases.clinical.gender"}},
         {"bool": {"must_not": [{"missing": {"field": "cases.clinical.gender"}}]}}]}}),
    # 14 - cases.clinical.gender not missing or cases.clinical.gender is missing
    ({"op": "or",
      "content": [
          {"op": "is",
           "content": {
               "field": "cases.clinical.gender",
               "value": "missing"
           }},
          {"op": "not",
           "content": {
               "field": "cases.clinical.gender",
               "value": "missing"
           }}]},
     {"bool": {"should": [
         {"missing": {"field": "cases.clinical.gender"}},
         {"bool": {"must_not": [{"missing": {"field": "cases.clinical.gender"}}]}}]}}),
    # 15 files.access != protected and (files.center.code = 01 and cases.project.primary_site = Brain
    ({"op": "and",
      "content": [
          {"op": "!=",
           "content": {"field": "files.access", "value": "protected"}},
          {"op": "and",
           "content": [
               {"op": "=",
                "content": {"field": "files.center.code", "value": "01"}},
               {"op": "=",
                "content": {"field": "cases.project.primary_site", "value": "Brain"}}]}]},
     {"bool": {
         "must_not": [{"terms": {"access": ["protected"]}}],
         "must": [
             {"terms": {"center.code": ["01"]}},
             {"terms": {"cases.project.primary_site": ["Brain"]}}]}}),
    # 16 - cases.clinical.age_at_diagnosis <= 20
    ({"op": "<=",
      "content": {
          "field": "cases.clinical.age_at_diagnosis",
          "value": ["20"]}
      },
     {"range": {"cases.clinical.age_at_diagnosis": {"lte": "20"}}}
     ),
    # 17 - cases.clinical.age_at_diagnosis <= 20
    ({"op": "and",
      "content": [{
          "op": "<=",
          "content": {
              "field": "cases.clinical.age_at_diagnosis",
              "value": ["20"]}}]},
     {"bool": {"must": [{"range": {"cases.clinical.age_at_diagnosis": {"lte": "20"}}}]}}
     ),
    # 18 - cases.clinical.age_at_diagnosis <= 30 and cases.clinical.age_at_diagnosis >= 20
    ({"op": "and",
      "content": [
          {"op": "<=",
           "content": {"field": "cases.clinical.age_at_diagnosis", "value": ["30"]}},
          {"op": ">=",
           "content": {"field": "cases.clinical.age_at_diagnosis", "value": ["20"]}
           }]},
     {"bool": {"must": [
         {"range": {"cases.clinical.age_at_diagnosis": {"lte": "30"}}},
         {"range": {"cases.clinical.age_at_diagnosis": {"gte": "20"}}}
     ]}}
     ),
    # 19 - cases.clinical.age_at_diagnosis <= 30 and cases.clinical.age_at_diagnosis >= 20
    #      and cases.clinical.days_to_death >= 100
    ({"op": "and",
      "content": [
          {"op": "<=",
           "content": {"field": "cases.clinical.age_at_diagnosis", "value": ["30"]}},
          {"op": ">=",
           "content": {"field": "cases.clinical.age_at_diagnosis", "value": ["20"]}
           },
          {"op": ">=",
           "content": {"field": "cases.clinical.days_to_death", "value": ["100"]}
           }]},
     {"bool": {"must": [
         {"range": {"cases.clinical.age_at_diagnosis": {"lte": "30"}}},
         {"range": {"cases.clinical.age_at_diagnosis": {"gte": "20"}}},
         {"range": {"cases.clinical.days_to_death": {"gte": "100"}}}
     ]}}
     )
])
def test_build_filters(filters, expected):
    assert request.build_filters("file", [], filters) == expected


@pytest.mark.parametrize("filters,expected", [
    # 0 - files.data_subtype = "Copy number segmentation"
    # and files.experimental_strategy = WGS
    # and cases.project.project_id = TCGA-BRCA
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.data_subtype", "value": "Copy number segmentation"}},
          {"op": "and",
           "content": [
               {"op": "=", "content": {"field": "files.experimental_strategy", "value": "WGS"}},
               {"op": "=", "content": {"field": "cases.project.project_id", "value": "TCGA-BRCA"}}]}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"terms": {"files.data_subtype": ["Copy number segmentation"]}},
                             {"terms": {"files.experimental_strategy": ["WGS"]}}
                         ]}},
                 "path": "files"}},
             {"terms": {"project.project_id": ["TCGA-BRCA"]}}]}}),
    # 1 - files.data_subtype = "Copy number segmentation"
    # and files.experimental_strategy = WGS
    # and cases.project.project_id = TCGA-BRCA
    # this is #0 simplified
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.data_subtype", "value": "Copy number segmentation"}},
          {"op": "=",
           "content": {"field": "files.experimental_strategy", "value": "WGS"}},
          {"op": "=",
           "content": {"field": "cases.project.project_id", "value": "TCGA-BRCA"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"terms": {"files.data_subtype": ["Copy number segmentation"]}},
                             {"terms": {"files.experimental_strategy": ["WGS"]}}]}},
                 "path": "files"}},
             {"terms": {"project.project_id": ["TCGA-BRCA"]}}]}}),
    # 2
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.access", "value": "open"}},
          {"op": "and",
           "content": [
               {"op": "=",
                "content": {"field": "files.center.code", "value": "01"}},
               {"op": "=",
                "content": {"field": "cases.project.primary_site", "value": "Brain"}}]}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"terms": {"files.access": ["open"]}},
                             {"terms": {"files.center.code": ["01"]}}]}},
                 "path": "files"}},
             {"terms": {"project.primary_site": ["Brain"]}}]}}),
    # 3
    ({"op": "and",
      "content": [
          {"op": "!=",
           "content": {"field": "files.access", "value": "protected"}},
          {"op": "and",
           "content": [
               {"op": "=",
                "content": {"field": "files.center.code", "value": "01"}},
               {"op": "=",
                "content": {"field": "cases.project.primary_site", "value": "Brain"}}]}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must_not": [{"terms": {"files.access": ["protected"]}}],
                         "must": [{"terms": {"files.center.code": ["01"]}}]}},
                 "path": "files"}},
             {"terms": {"project.primary_site": ["Brain"]}}]}}),
    # 4
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.access", "value": "protected4"}},
          {"op": "and",
           "content": [
               {"op": "!=",
                "content": {"field": "files.center.code", "value": "04"}}]}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [{"terms": {"files.access": ["protected4"]}}],
                         "must_not": [{"terms": {"files.center.code": ["04"]}}]}},
                 "path": "files"}}]}}),
    # 5
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.access", "value": "protected"}},
          {"op": "and",
           "content": [
               {"op": "!=",
                "content": {"field": "files.center.code", "value": "01"}},
               {"op": "=",
                "content": {"field": "cases.project.primary_site", "value": "Brain"}}]}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [{"terms": {"files.access": ["protected"]}}],
                         "must_not": [{"terms": {"files.center.code": ["01"]}}]}},
                 "path": "files"}},
             {"terms": {"project.primary_site": ["Brain"]}}]}}),
    # 6 - nested nested =
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.foo.name", "value": "cname"}},
          {"op": "=",
           "content": {"field": "files.foo.code", "value": "01"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must": [
                                             {"terms": {"files.foo.name": ["cname"]}},
                                             {"terms": {"files.foo.code": ["01"]}}
                                         ]}},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}}),
    # 7
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.foo.name", "value": "cname"}},
          {"op": "!=",
           "content": {"field": "files.foo.code", "value": "01"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must": [
                                             {"terms": {"files.foo.name": ["cname"]}}
                                         ],
                                         "must_not": [
                                             {"terms": {"files.foo.code": ["01"]}}
                                         ],
                                     }},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}}),
    # 8
    ({"op": "and",
      "content": [
          {"op": "!=",
           "content": {"field": "files.foo.name", "value": "cname"}},
          {"op": "=",
           "content": {"field": "files.foo.code", "value": "01"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must": [
                                             {"terms": {"files.foo.code": ["01"]}}
                                         ],
                                         "must_not": [
                                             {"terms": {"files.foo.name": ["cname"]}}
                                         ],
                                     }},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}}),
    # 9 - nested nested !=
    ({"op": "and",
      "content": [
          {"op": "!=",
           "content": {"field": "files.foo.name", "value": "cname"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must_not": [
                                             {"terms": {"files.foo.name": ["cname"]}}
                                         ],
                                     }},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}}),
    # 10 - nested nested exclude
    ({"op": "exclude",
      "content": {"field": "files.foo.code", "value": ['01']}},
     {"nested": {
         "filter": {
             "bool": {
                 "must": [
                     {"nested": {
                         "filter": {
                             "bool": {
                                 "must_not": [
                                     {"terms": {"files.foo.code": ['01']}}
                                 ],
                             }},
                         "path": "files.foo"
                     }}]}},
         "path": "files"}}),
    # 11 - nested nested range
    ({"op": "and",
      "content": [
          {"op": ">=",
           "content": {"field": "files.foo.name", "value": 7}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must": [
                                             {"range": {"files.foo.name": {"gte": 7}}}
                                         ],
                                     }},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}}),
    # 12 - nested nested nested =
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.foo.bar.name", "value": "cname"}},
          {"op": "=",
           "content": {"field": "files.foo.bar.code", "value": "01"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must": [
                                             {"nested": {
                                                 "filter": {
                                                     "bool": {
                                                         "must": [
                                                             {"terms": {"files.foo.bar.name": ["cname"]}},
                                                             {"terms": {"files.foo.bar.code": ["01"]}}
                                                         ]}},
                                                 "path": "files.foo.bar"
                                             }}]}},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}}),
    # 13 - nested nested nested !=
    ({"op": "and",
      "content": [
          {"op": "!=",
           "content": {"field": "files.foo.bar.name", "value": "cname"}},
          {"op": "=",
           "content": {"field": "files.foo.bar.code", "value": "01"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must": [
                                             {"nested": {
                                                 "filter": {
                                                     "bool": {
                                                         "must": [
                                                             {"terms": {"files.foo.bar.code": ["01"]}}
                                                         ],
                                                         "must_not": [
                                                             {"terms": {"files.foo.bar.name": ["cname"]}}
                                                         ]}},
                                                 "path": "files.foo.bar"
                                             }}]}},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}}),
    # 14 - nested not_nested nested
    ({"op": "and",
      "content": [
          {"op": "!=",
           "content": {"field": "files.nn.baz.name", "value": "cname"}},
          {"op": "=",
           "content": {"field": "files.code", "value": "beep"}}
      ]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must_not": [
                                             {"terms": {"files.nn.baz.name": ["cname"]}}
                                         ],
                                     }},
                                 "path": "files.nn.baz"
                             }},
                             {"terms": {"files.code": ["beep"]}}]}},
                 "path": "files"}}]}}),
    # 15 - is missing
    ({"op": "and",
      "content": [
          {"op": "in", "content": {"field": "files.data_category", "value": ["Simple Nucleotide Variation"]}},
          {"op": "in", "content": {"field": "files.experimental_strategy", "value": ["WXS"]}},
          {"op": "is", "content": {"field": "files.analysis.metadata.read_groups.is_paired_end",
                                   "value": "MISSING"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"terms": {"files.data_category": ["Simple Nucleotide Variation"]}},
                             {"terms": {"files.experimental_strategy": ["WXS"]}},
                             {"missing": {"field": "files.analysis.metadata.read_groups.is_paired_end"}}
                         ]}},
                 "path": "files"}}]}}),
    # 16 - not missing
    ({"op": "and",
      "content": [
          {"op": "in", "content": {"field": "files.data_category", "value": ["Simple Nucleotide Variation"]}},
          {"op": "in", "content": {"field": "files.experimental_strategy", "value": ["WXS"]}},
          {"op": "not", "content": {"field": "files.analysis.metadata.read_groups.is_paired_end",
                                    "value": "MISSING"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"terms": {"files.data_category": ["Simple Nucleotide Variation"]}},
                             {"terms": {"files.experimental_strategy": ["WXS"]}},
                             {"bool": {"must_not": [
                                 {"missing": {"field": "files.analysis.metadata.read_groups.is_paired_end"}}
                             ]}}
                         ]}},
                 "path": "files"}}]}})
])
def test_build_filters_nested(filters, expected):
    nested_fields = ['files', 'files.foo', 'files.foo.bar', 'files.nn.baz']
    assert request.build_filters('case', nested_fields, filters) == expected


@pytest.mark.parametrize("filters,expected", [
    # 0
    ({"op": "in",
      "content": {"field": "cases.case_id", "value": ["006*"]}},
     {"regexp": {"case_id.raw": "006.*"}}),
    # 1
    ({"op": "and",
      "content": [
          {"op": "in",
           "content":
               {"field": "cases.case_id", "value": ["006*"]}}]},
     {"bool":
         {"must": [
             {"regexp": {"case_id.raw": "006.*"}}]}}),
    # 2
    ({"op": "in",
      "content":
          {"field": "cases.case_id", "value": ["006*", "v1"]}},
     {"bool":
         {"should": [
             {"terms": {"case_id.raw": ["v1"]}},
             {"regexp": {"case_id.raw": "006.*"}}]}}),
    # 3
    ({"op": "and",
      "content": [
          {"op": "in",
           "content":
               {"field": "cases.case_id", "value": ["006*", "v1"]}}]},
     {"bool":
         {"should": [
             {"bool":
                 {"should": [
                     {"terms": {"case_id.raw": ["v1"]}},
                     {"regexp": {"case_id.raw": "006.*"}}]}}]}}),
    # 4
    ({"op": "and",
      "content": [
          {"op": "in",
           "content":
               {"field": "cases.case_id", "value": ["006*", "v1"]}},
          {"op": "in",
           "content":
               {"field": "cases.project.primary_site", "value": ["Brain"]}}]},
     {"bool":
         {
             "must": [
                 {"terms": {"project.primary_site": ["Brain"]}}
             ],
             "should": [
                 {"bool":
                     {"should": [
                         {"terms": {"case_id.raw": ["v1"]}},
                         {"regexp": {"case_id.raw": "006.*"}}]}}]}}),
    # 5 - nested nested wildcard
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.foo.name", "value": "cname*"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must": [
                                             {"regexp": {"files.foo.name": "cname.*"}}
                                         ],
                                     }},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}}),
    # 6 - multi in same field
    ({"op": "and",
      "content": [
          {"op": "in",
           "content": {"field": "files.foo.name", "value": ["*cname", "cn*me", "cname*"]}}]},
     {"bool": {
         "should": [
             {"bool": {
                 "should": [
                     {"nested": {
                         "filter": {
                             "bool": {
                                 "must": [
                                     {"nested": {
                                         "filter": {
                                             "bool": {"must": [{"regexp": {"files.foo.name": ".*cname"}}]}},
                                         "path": "files.foo"
                                     }},
                                 ]}},
                         "path": "files"}},
                     {"nested": {
                         "filter": {
                             "bool": {
                                 "must": [
                                     {"nested": {
                                         "filter": {
                                             "bool": {"must": [{"regexp": {"files.foo.name": "cn.*me"}}]}},
                                         "path": "files.foo"
                                     }},
                                 ]}},
                         "path": "files"}},
                     {"nested": {
                         "filter": {
                             "bool": {
                                 "must": [
                                     {"nested": {
                                         "filter": {
                                             "bool": {"must": [{"regexp": {"files.foo.name": "cname.*"}}]}},
                                         "path": "files.foo"
                                     }},
                                 ]}},
                         "path": "files"}}
                 ]
             }}
         ]}}),
    # 8 - multi diff fields
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "files.foo.name1", "value": "*cname"}},
          {"op": "=",
           "content": {"field": "files.foo.name2", "value": "cn*me"}},
          {"op": "=",
           "content": {"field": "files.foo.name3", "value": "cname*"}}]},
     {"bool": {
         "must": [
             {"nested": {
                 "filter": {
                     "bool": {
                         "must": [
                             {"nested": {
                                 "filter": {
                                     "bool": {
                                         "must": [
                                             {"regexp": {"files.foo.name1": ".*cname"}},
                                             {"regexp": {"files.foo.name2": "cn.*me"}},
                                             {"regexp": {"files.foo.name3": "cname.*"}}
                                         ],
                                     }},
                                 "path": "files.foo"
                             }}]}},
                 "path": "files"}}]}})
])
def test_build_filters_wildcard(filters, expected):
    assert request.build_filters('case', ['files', 'files.foo'], filters) == expected


@pytest.mark.parametrize("filters,expected", [
    # 0
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "f1", "value": "v1"}},
          {"op": "and",
           "content": [
               {"op": "=", "content": {"field": "f2", "value": "v2"}},
               {"op": "=", "content": {"field": "f3", "value": "v3"}}]}]},
     {"op": "and",
      "content": [
          {"op": "in",
           "content": {"field": "f1", "value": ["v1"]}},
          {"op": "in",
           "content": {"field": "f2", "value": ["v2"]}},
          {"op": "in",
           "content": {"field": "f3", "value": ["v3"]}}]}),
    # 1
    ({"op": "and",
      "content": [
          {"op": "=",
           "content": {"field": "f1", "value": "v1"}},
          {"op": "and",
           "content": [
               {"op": "=", "content": {"field": "f2", "value": "v2"}},
               {"op": "and",
                "content": [
                    {"op": "=", "content": {"field": "f3", "value": "v3"}},
                    {"op": "=", "content": {"field": "f4", "value": "v4"}}]}]}]},
     {"op": "and",
      "content": [
          {"op": "in",
           "content": {"field": "f1", "value": ["v1"]}},
          {"op": "in",
           "content": {"field": "f2", "value": ["v2"]}},
          {"op": "in",
           "content": {"field": "f3", "value": ["v3"]}},
          {"op": "in",
           "content": {"field": "f4", "value": ["v4"]}}]}),
    # 2
    ({"op": "or",
      "content": [
          {"op": "=",
           "content": {"field": "f1", "value": "v1"}},
          {"op": "or",
           "content": [
               {"op": "=", "content": {"field": "f2", "value": "v2"}},
               {"op": "or",
                "content": [
                    {"op": "=", "content": {"field": "f3", "value": "v3"}},
                    {"op": "=", "content": {"field": "f4", "value": "v4"}}]}]}]},
     {"op": "or",
      "content": [
          {"op": "in",
           "content": {"field": "f1", "value": ["v1"]}},
          {"op": "in",
           "content": {"field": "f2", "value": ["v2"]}},
          {"op": "in",
           "content": {"field": "f3", "value": ["v3"]}},
          {"op": "in",
           "content": {"field": "f4", "value": ["v4"]}}]}),
    # 3
    ({"op": "or",
      "content": [
          {"op": "=",
           "content": {"field": "f1", "value": "v1"}},
          {"op": "and",
           "content": [
               {"op": "=", "content": {"field": "f2", "value": "v2"}},
               {"op": "and",
                "content": [
                    {"op": "=", "content": {"field": "f3", "value": "v3"}},
                    {"op": "=", "content": {"field": "f4", "value": "v4"}}]}]}]},
     {"op": "or",
      "content": [
          {"op": "in",
           "content": {"field": "f1", "value": ["v1"]}},
          {"op": "and",
           "content": [
               {"op": "in",
                "content": {"field": "f2", "value": ["v2"]}},
               {"op": "in",
                "content": {"field": "f3", "value": ["v3"]}},
               {"op": "in",
                "content": {"field": "f4", "value": ["v4"]}}]}]}),
    # 4
    ({"op": "in",
      "content": {"field": "f1", "value": ["v1", "v2"]}},
     {"op": "in",
      "content": {"field": "f1", "value": ["v1", "v2"]}}),
    # 5
    ({"op": "exclude",
      "content": {"field": "f1", "value": ["v1", "v2"]}},
     {"op": "exclude",
      "content": {"field": "f1", "value": ["v1", "v2"]}}),
    # 6
    ({"op": "=",
      "content": {"field": "f1", "value": "v1"}},
     {"op": "in",
      "content": {"field": "f1", "value": ["v1"]}}),
    # 7
    ({"op": "!=",
      "content": {"field": "f1", "value": "v1"}},
     {"op": "exclude",
      "content": {"field": "f1", "value": ["v1"]}}),
    # 8
    ({"op": "in",
      "content": {"field": "f1", "value": ["v1", "xyz*"]}},
     {"op": "or",
      "content": [
          {"op": "in", "content": {"field": "f1", "value": ["v1"]}},
          {"op": "in", "content": {"field": "f1", "value": ["xyz*"]}}
      ]}),
    # 9 - wrapped with "and"
    ({"op": "and",
      "content": [
          {"op": "in",
           "content": {"field": "f1", "value": ["xyz*", "v1"]}}]},
     {"op": "and",
      "content": [
          {"op": "or",
           "content": [
               {"op": "in", "content": {"field": "f1", "value": ["v1"]}},
               {"op": "in", "content": {"field": "f1", "value": ["xyz*"]}}]}]})
])
def test_filter_optimizer(filters, expected):
    assert request.filter_optimizer(filters) == expected
