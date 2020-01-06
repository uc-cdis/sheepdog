import pytest
from sheepdog.utils.transforms.bcr_xml_to_json import munge_property


def test_gdc_type_mappings():

    # parse float
    float_val = munge_property("38.3", "float")
    assert isinstance(float_val, float)
    with pytest.raises(ValueError):
        munge_property("38k", "float")

    # parse int
    int_val = munge_property("38", "int")
    assert isinstance(int_val, int)
    with pytest.raises(ValueError):
        munge_property("4.9", "int")

    # parse long
    long_val = munge_property("3111118", "long")
    assert isinstance(long_val, int)
    with pytest.raises(ValueError):
        munge_property("4.9", "long")

    # parse str
    str_val = munge_property("Dummy Text", "str")
    assert isinstance(str_val, str)
    assert str_val == "Dummy Text"

    # parse str lower
    lower_val = munge_property("Dummy Text", "str.lower")
    assert isinstance(lower_val, str)
    assert lower_val == "dummy text"

    # parse bool
    assert munge_property("yes", "bool")
    assert munge_property("true", "bool")
    assert not munge_property("no", "bool")
    assert not munge_property("false", "bool")

    with pytest.raises(ValueError):
        munge_property("NAY", "bool")
