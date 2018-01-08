import pytest

from peregrine.esutils import response


@pytest.mark.parametrize("aggs,expected", [
    (
      {
        "status_global": {
          "status_filtered": {
            'status': {'buckets': [{'key': 'legacy', 'doc_count': 34}], 'sum_other_doc_count': 0, 'doc_count_error_upper_bound': 0}
          }
        }
      },
      {'status': {'buckets': [{'key': 'legacy', 'doc_count': 34}]}}
    ),
    (
      {
        "categoryName_missing": {"doc_count": 0},
        "itemType_missing": {"doc_count": 0},
        "itemType_global": {
          "itemType_filtered": {
            "itemType": {
              "buckets": [{"key": "Aliquot", "doc_count": 16730}, {"key": "Portion", "doc_count": 8}],
              "sum_other_doc_count": 0,
              "doc_count_error_upper_bound": 0
            }
          }
        },
        "categoryName_global": {
          "categoryName_filtered": {
            "categoryName": {
              "buckets": [
                {"key": "Item flagged DN", "doc_count": 16191},
                {"key": "Item is noncanonical", "doc_count": 2923},
                {"key": "Sample compromised", "doc_count": 2},
                {"key": "WGA Failure", "doc_count": 1}
              ],
              "sum_other_doc_count": 0, "doc_count_error_upper_bound": 0
            }
          }
        }
      },
      {'categoryName': {'buckets': [{'doc_count': 16191, 'key': 'Item flagged DN'},
                                   {'doc_count': 2923, 'key': 'Item is noncanonical'},
                                   {'doc_count': 2, 'key': 'Sample compromised'},
                                   {'doc_count': 1, 'key': 'WGA Failure'}]},
       'itemType': {'buckets': [{'doc_count': 16730, 'key': 'Aliquot'},
                               {'doc_count': 8, 'key': 'Portion'}]}}
    ),
    (
      {
        "status_global": {
          "status_filtered": {
            'status': {'buckets': [{'key': 'legacy', 'doc_count': 34}], 'sum_other_doc_count': 0,
                             'doc_count_error_upper_bound': 0},
            'status_missing': {'doc_count': 1}
          }
        }
      },
      {'status': {'buckets': [{'key': 'legacy', 'doc_count': 34}, {'key': '_missing', 'doc_count': 1}]}}
    ),
    (
      {
        "archive.revision_global": {
          "archive.revision_filtered": {
            "archive.revision": {"buckets": [{"doc_count": 37860, "key": 2002}, {"doc_count": 36684, "key": 0}],
                           'sum_other_doc_count': 0, 'doc_count_error_upper_bound': 0}
          }
        },
        'status_missing': {'doc_count': 0}
      },
      {"archive.revision": {"buckets": [{"doc_count": 37860, "key": 2002}, {"doc_count": 36684, "key": 0}]}}
    ),
    (
      {
        "samples.is_ffpe_global": {
          "doc_count": 1234,
          "samples.is_ffpe_filtered":{
            "doc_count": 132,
            "samples": {
              "doc_count": 123,
              "is_ffpe_missing": {
                "doc_count": 3,
                "is_ffpe_missing_rn": {
                  "doc_count": 5
                }
              },
              "is_ffpe": {
                "buckets": [
                  {
                    "key": "a",
                    "doc_count": 7,
                    "is_ffpe_rn": {
                      "doc_count": 7
                    }
                  }
                ]
              }
            }
          }
        }
      },
      {'samples.is_ffpe': {'buckets': [{'key': 'a', 'doc_count': 7},
      {'key': '_missing', 'doc_count': 5}]}}
    ),
    (
      {
        "samples.portions.amount_global": {
          "doc_count": 1234,
          "samples.portions.amount_filtered":{
            "doc_count": 132,
            "samples": {
              "doc_count": 123,
              "portions": {
                "doc_count": 123,
                "amount_missing": {
                  "doc_count": 3,
                  "amount_missing_rn": {
                    "doc_count": 5
                  }
                },
                "amount": {
                  "buckets": [
                    {
                      "key": "a",
                      "doc_count": 7,
                      "amount_rn": {
                        "doc_count": 7
                      }
                    }
                  ]
                }
              }
            }
          }
        }
      },
      {'samples.portions.amount': {'buckets': [{'key': 'a', 'doc_count': 7}, {'key': '_missing', 'doc_count': 5}]}}),
    (
      {
        "samples.portions.amount_global": {
          "doc_count": 1234,
          "samples.portions.amount_filtered":{
            "doc_count": 132,
            "samples": {
              "doc_count": 123,
              "portions": {
                "doc_count": 123,
                "amount_missing": {
                  "doc_count": 3,
                  "amount_missing_rn": {
                    "doc_count": 5
                  }
                },
                "amount": {
                  "buckets": [
                    {
                      "key": "a",
                      "doc_count": 7,
                      "amount_rn": {
                        "doc_count": 7
                      }
                    }
                  ]
                }
              },
              "is_ffpe_missing": {
                "doc_count": 3,
                "is_ffpe_missing_rn": {
                  "doc_count": 5
                }
              },
              "is_ffpe": {
                "buckets": [
                  {
                    "key": "a",
                    "doc_count": 7,
                    "is_ffpe_rn": {
                      "doc_count": 7
                    }
                  }
                ]
              }
            }
          }
        }
      },
      {'samples.portions.amount': {'buckets': [{'key': 'a', 'doc_count': 7},
      {'key': '_missing', 'doc_count': 5}]},
      'samples.is_ffpe': {'buckets': [{'key': 'a', 'doc_count': 7},
      {'key': '_missing', 'doc_count': 5}]}}),
    (
      {
        'status_global': {
          "status_filtered": {
            "status": {
              'buckets': [{'key': 'legacy', 'doc_count': 34}],
              'sum_other_doc_count': 0,
              'doc_count_error_upper_bound': 0
            },
            'status_missing': {'doc_count': 0}
          }
        },
        "samples.portions.amount_global": {
          "doc_count": 1234,
          "samples.portions.amount_filtered":{
            "doc_count": 132,
            "samples": {
              "doc_count": 123,
              "portions": {
                "doc_count": 123,
                "amount_missing": {
                  "doc_count": 3,
                  "amount_missing_rn": {
                    "doc_count": 5
                  }
                },
                "amount": {
                  "buckets": [
                    {
                      "key": "a",
                      "doc_count": 7,
                      "amount_rn": {
                        "doc_count": 7
                      }
                    }
                  ]
                }
              },
              "is_ffpe_missing": {
                "doc_count": 3,
                "is_ffpe_missing_rn": {
                  "doc_count": 5
                }
              },
              "is_ffpe": {
                "buckets": [
                  {
                    "key": "a",
                    "doc_count": 7,
                    "is_ffpe_rn": {
                      "doc_count": 7
                    }
                  }
                ]
              }
            }
          }
        }
      },
      {'status': {'buckets': [{'key': 'legacy', 'doc_count': 34}]},
       'samples.portions.amount': {'buckets': [{'key': 'a', 'doc_count': 7}, {'key': '_missing', 'doc_count': 5}]},
       'samples.is_ffpe': {'buckets': [{'key': 'a', 'doc_count': 7}, {'key': '_missing', 'doc_count': 5}]}}
)

])
def test_parse_aggregations(aggs, expected):
    assert response.parse_aggregations(["samples", "samples.portions"], {}, aggs) == expected


@pytest.mark.parametrize("bucket,data,field,expected", [
    ({'key': 'P1', 'case_count': 10},
    {
        'project.project_id': {
            'buckets': [{
                'file_size': 20,
                'key': 'P1',
                'doc_count': 30
            }]
        }
    },
    'project.project_id',
    {'file_size': 20, 'key': 'P1', 'doc_count': 30})
])
def test_match_bucket(bucket, data, field, expected):
    assert next(response.match_bucket(bucket, data, field), {}) == expected


@pytest.mark.parametrize("bucket,data,field,expected", [
    ({'key': 'P1', 'case_count': 10},
    {
        'project.project_id': {
            'buckets': [{
                'file_size': 20,
                'key': 'P1',
                'doc_count': 30
            }]
        }
    },
    'project.project_id',
    [('case_count', 10), ('key', 'P1'), ('doc_count', 30), ('key', 'P1'), ('file_size', 20)])
])
def test_zip_buckets(bucket, data, field, expected):
    assert response.zip_buckets(bucket, data, field) == expected


@pytest.mark.parametrize("bucket,data,field,expected", [
    ({'key': 'P1', 'case_count': 10},
    {
        'project.project_id': {
            'buckets': [{
                'file_size': 20,
                'key': 'P1',
                'doc_count': 30
            }]
        }
    },
    'project.project_id',
    {'case_count': 10, 'doc_count': 30, 'file_size': 20, 'key': 'P1'})
])
def test_merge_buckets(bucket, data, field, expected):
    assert response.merge_buckets(bucket, data, field) == expected


@pytest.mark.parametrize("d1,d2,field,expected", [
    ({
        'project.project_id': {
            'buckets': [{
                'key': 'P1',
                'case_count': 10
            }]
        }
    },
    {
        'project.project_id': {
            'buckets': [{
                'file_size': 20,
                'key': 'P1',
                'doc_count': 30
            }]
        }
    },
    'project.project_id',
    [{'case_count': 10, 'doc_count': 30, 'file_size': 20, 'key': 'P1'}])
])
def test_spread_buckets(d1, d2, field, expected):
    assert response.spread_buckets(d1, d2, field) == expected


@pytest.mark.parametrize("case_data,file_data,expected", [
    ({
        'project.project_id': {
            'buckets': [{
                'key': 'P1',
                'case_count': 10
            }]
        }
    },
    {
        'project.project_id': {
            'buckets': [{
                'file_size': 20,
                'key': 'P1',
                'doc_count': 30
            }]
        }
    },
    {
        'fs': 0,
        'project.project_id': {
            'buckets': [{
                'case_count': 10,
                'file_size': 20,
                'key': 'P1',
                'doc_count': 30
            }]
        }
    })
])
def test_merge_summary(case_data, file_data, expected):
    fields = ["project.project_id"]
    assert response.merge_summary(case_data, file_data, fields) == expected
