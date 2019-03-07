import os

from lxml import etree
import pytest


DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')


@pytest.fixture(scope="function", params=["clinical_sample_1.xml", "clinical_sample_2.xml"])
def xml_fixture(request):

    with open("{}/{}".format(DATA_DIR, request.param), "r+") as xml:
        yield etree.fromstring(xml.read())

