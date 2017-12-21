import pytest

from peregrine.utils.response import to_xml, striptags_from_dict


@pytest.mark.parametrize("data,expected", [
    ([{'a': '0'}], "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><response><item><a>0</a></item></response>"),
    ([{'a': '0', 'b': 1}],
     "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><response><item><a>0</a><b>1</b></item></response>"),
    ([{'a': '0'}, {'b': 1}],
     "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><response><item><a>0</a></item><item><b>1</b></item></response>"),
    ([{'summary': {'experimental_strategies': [{'experimental_strategy': 'Methylation array'},
                                               {'experimental strategy': 'miRNA-Seq'}]}}],
     "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><response><item><summary><experimental_strategies><item><experimental_strategy>Methylation array</experimental_strategy></item><item><experimental_strategy>miRNA-Seq</experimental_strategy></item></experimental_strategies></summary></item></response>")
])
def test_to_xml(data, expected):
    data = to_xml({}, data)

    assert data == expected


@pytest.mark.parametrize("data,expected", [
    ({"<a>b</a>": "<a>d</a>"}, {"b": "d"}),
    ({"a": {"<a>b</a>": "<a>c</a>"}}, {"a": {"b": "c"}}),
    ({"<a>a</a>": {"<a>b</a>": {"<a>c</a>": "<a>d</a>"}}}, {"a": {"b": {"c": "d"}}}),
    ({"<a><a>a</a>b</a>": "c"}, {"ab": "c"}),
    ({"a": 1}, {"a": 1}),
])
def test_striptags_from_response(data, expected):
    assert striptags_from_dict(data) == expected
