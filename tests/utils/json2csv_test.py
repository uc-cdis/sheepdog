import pytest

from peregrine.utils.json2csv import list_to_obj, flatten_nested_obj, flatten_obj, row_with_headers, rows_with_headers, \
    to_csv


@pytest.mark.parametrize("k,v,expected", [
    ('k0', ['v0', 'v1'], {'k0_0': 'v0', 'k0_1': 'v1'})
])
def test_list_to_obj(k, v, expected):
    assert list_to_obj(k, v) == expected


@pytest.mark.parametrize("k,v,expected", [
    ('k0', {'k1': 'v1', 'k2': 'v2'}, {'k0_k1': 'v1', 'k0_k2': 'v2'})
])
def test_flatten_nested_json(k, v, expected):
    assert flatten_nested_obj(k, v) == expected


@pytest.mark.parametrize("json,expected", [
    # 0 - value
    ({'k0': 'v0'}, {'k0': 'v0'}),
    # 1 - list
    ({'k0': ['v0', 'v1']}, {'k0_0': 'v0', 'k0_1': 'v1'}),
    # 2 - obj
    ({'k0': {'k1': 'v1', 'k2': 'v2'}}, {'k0_k1': 'v1', 'k0_k2': 'v2'}),
    # 3 - list of obj
    ({'k0': [{'k1': 'v1', 'k2': 'v2'}, {'k1': 'v3', 'k2': 'v4'}]},
     {'k0_0_k1': 'v1', 'k0_0_k2': 'v2', 'k0_1_k1': 'v3', 'k0_1_k2': 'v4'}),
    # 4 - multi key list of obj
    ({
         'k0': [
             {'k01': 'v01', 'k02': 'v02'},
             {'k01': 'v03', 'k02': 'v04'}
         ],
         'k1': [
             {'k11': 'v11', 'k12': 'v12'},
             {'k11': 'v13', 'k12': 'v14'}
         ],
     }, {
         'k0_0_k01': 'v01',
         'k0_0_k02': 'v02',
         'k0_1_k01': 'v03',
         'k0_1_k02': 'v04',
         'k1_0_k11': 'v11',
         'k1_0_k12': 'v12',
         'k1_1_k11': 'v13',
         'k1_1_k12': 'v14',
     }),
    # 5 - combination nested lists and obj
    ({
         'k0': [
             {
                 'k01': ['v010', 'v011'],
                 'k02': {
                     'k10': 'v02'
                 }
             },
             {
                 'k01': ['v012', 'v013'],
                 'k02': {
                     'k10': 'v04'
                 }
             }
         ],
     }, {
         'k0_0_k01_0': 'v010',
         'k0_0_k01_1': 'v011',
         'k0_0_k02_k10': 'v02',
         'k0_1_k01_0': 'v012',
         'k0_1_k01_1': 'v013',
         'k0_1_k02_k10': 'v04',
     })
])
def test_flatten_obj(json, expected):
    assert flatten_obj(json) == expected


@pytest.mark.parametrize("acc,json,expected", [
    # 0 - value
    (([], set()), {'k0': 'v0'}, ([{'k0': 'v0'}], {'k0'})),
    # 1 - list
    (([], set()), {'k0': ['v0', 'v1']}, ([{'k0_0': 'v0', 'k0_1': 'v1'}], {'k0_0', 'k0_1'})),
    # 2 - obj
    (([], set()), {'k0': {'k1': 'v1', 'k2': 'v2'}}, ([{'k0_k1': 'v1', 'k0_k2': 'v2'}], {'k0_k1', 'k0_k2'})),
    # 3 - append with same keys
    (([{'k0_k1': 'v1', 'k0_k2': 'v2'}], {'k0_k1', 'k0_k2'}), {'k0': {'k1': 'v1', 'k2': 'v2'}},
     ([{'k0_k1': 'v1', 'k0_k2': 'v2'}, {'k0_k1': 'v1', 'k0_k2': 'v2'}], {'k0_k1', 'k0_k2'})),
    # 4 - append with missing keys
    (([{'k0_k1': 'v1', 'k0_k2': 'v2', 'k0_k3': 'v3'}], {'k0_k1', 'k0_k2', 'k0_k3'}), {'k0': {'k1': 'v1', 'k2': 'v2'}},
     ([{'k0_k1': 'v1', 'k0_k2': 'v2', 'k0_k3': 'v3'}, {'k0_k1': 'v1', 'k0_k2': 'v2'}], {'k0_k1', 'k0_k2', 'k0_k3'})),
    # 5 - append with extra keys
    (([{'k0_k1': 'v1', 'k0_k2': 'v2'}], {'k0_k1', 'k0_k2'}), {'k0': {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}},
     ([{'k0_k1': 'v1', 'k0_k2': 'v2'}, {'k0_k1': 'v1', 'k0_k2': 'v2', 'k0_k3': 'v3'}], {'k0_k1', 'k0_k2', 'k0_k3'})),
])
def test_row_with_headers(acc, json, expected):
    assert row_with_headers(acc, json) == expected


@pytest.mark.parametrize("hits,expected", [
    # 0
    ([{'k0': 'v0'}], ([{'k0': 'v0'}], {'k0'})),
    # 1
    ([{'k0': {'k1': 'v1', 'k2': 'v2'}}, {'k0': {'k1': 'v1', 'k2': 'v2'}}],
     ([{'k0_k1': 'v1', 'k0_k2': 'v2'}, {'k0_k1': 'v1', 'k0_k2': 'v2'}], {'k0_k1', 'k0_k2'})),
    # 2
    ([{'k0': {'k1': 'v1', 'k2': 'v2'}}, {'k0': {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}}],
     ([{'k0_k1': 'v1', 'k0_k2': 'v2'}, {'k0_k1': 'v1', 'k0_k2': 'v2', 'k0_k3': 'v3'}], {'k0_k1', 'k0_k2', 'k0_k3'}))
])
def test_rows_with_headers(hits, expected):
    assert rows_with_headers(hits) == expected


@pytest.mark.parametrize("hits,expected", [
    # 0
    ([{'k0': 'v0'}], 'k0\r\nv0\r\n'),
    # 1
    ([{'k0': {'k1': 'v1', 'k2': 'v2'}}, {'k0': {'k1': 'v3', 'k2': 'v4'}}],
     'k0_k1,k0_k2\r\nv1,v2\r\nv3,v4\r\n'),
    # 2
    ([{'k0': {'k1': 'v1', 'k2': 'v2'}}, {'k0': {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}}],
     'k0_k1,k0_k2,k0_k3\r\nv1,v2,\r\nv1,v2,v3\r\n'),
    # 3
    ([{'k0': ['v1', 'v2']}, {'k0': ['v1', 'v2']}],
     'k0_0,k0_1\r\nv1,v2\r\nv1,v2\r\n'),
    # 4
    ([{'k0': ['v1', 'v2', 'v3']}, {'k0': ['v1', 'v2']}],
     'k0_0,k0_1,k0_2\r\nv1,v2,v3\r\nv1,v2,\r\n'),
    # 5
    ([{'k0': ['v1', 'v2']}, {'k0': ['v1', 'v2', 'v3']}],
     'k0_0,k0_1,k0_2\r\nv1,v2,\r\nv1,v2,v3\r\n'),
    # 6
    ([{'k0': {'k1': ['v1', 'v2']}}, {'k0': {'k1': ['v1', 'v2']}}],
     'k0_k1_0,k0_k1_1\r\nv1,v2\r\nv1,v2\r\n'),
    # 7
    ([{'k0': [{'k1': 'v1'}, {'k1': 'v2'}]}, {'k0': [{'k1': 'v1'}, {'k1': 'v2'}]}],
     'k0_0_k1,k0_1_k1\r\nv1,v2\r\nv1,v2\r\n'),
    # 8
    ([{'k0': {'k1': 'v1', 'k2': ['v20', 'v21'], 'k3': {'k30': 'v30', 'k31': 'v31'}}},
      {'k0': {'k1': 'v1', 'k2': ['v20', 'v21'], 'k3': {'k30': 'v30', 'k31': 'v31'}}}],
     'k0_k2_1,k0_k2_0,k0_k3_k30,k0_k3_k31,k0_k1\r\nv21,v20,v30,v31,v1\r\nv21,v20,v30,v31,v1\r\n'),
    # 9
    ([{'k0': {'k2': ['v20', 'v21'], 'k3': {'k30': 'v30', 'k31': 'v31'}}},
      {'k0': {'k1': 'v1', 'k2': ['v20'], 'k3': {'k30': 'v30'}}}],
     'k0_k2_1,k0_k2_0,k0_k3_k30,k0_k3_k31,k0_k1\r\nv21,v20,v30,v31,\r\n,v20,v30,,v1\r\n')
])
def test_to_csv(hits, expected):
    assert to_csv(hits) == expected


@pytest.mark.parametrize("hits,expected", [
    ([{'k0': {'k1': 'v1', 'k2': 'v2'}}, {'k0': {'k1': 'v3', 'k2': 'v4'}}],
     'k0_k1\tk0_k2\r\nv1\tv2\r\nv3\tv4\r\n'),
])
def test_to_csv_excel_tab(hits, expected):
    assert to_csv(hits, dialect='excel-tab') == expected
