from lxml import etree
import pytest

from sheepdog.utils.transforms import BcrClinicalXmlToJsonParser
from sheepdog.xml import EvaluatorFactory
from sheepdog.xml.evaluators.fields import BasicEvaluator, FilterElementEvaluator, LastFollowUpEvaluator, \
    TreatmentTherapyEvaluator, VitalStatusEvaluator


@pytest.mark.parametrize("evaluator, expected", [
        ({}, BasicEvaluator),
        (dict(name="filter"), FilterElementEvaluator),
        (dict(name="last_follow_up"), LastFollowUpEvaluator),
        (dict(name="vital_status"), VitalStatusEvaluator),
        (dict(name="treatment_therapy"), TreatmentTherapyEvaluator),
])
def test_evaluator_factory(xml_fixture, evaluator, expected):

    xml = etree.fromstring(xml_fixture)
    nspace = xml.nsmap
    ev = EvaluatorFactory.get_instance(xml, nspace, dict(evaluator=evaluator))
    assert isinstance(ev, expected)


def test_bcr_parsing(xml_fixture):

    parser = BcrClinicalXmlToJsonParser(project_code=None)
    parser.loads(xml_fixture)

    # 5 nodes expected
    # 1 demographics, 1 diagnosis, 1 exposure, 2 treatment
    assert 5 == len(parser.docs)

    nodes_found = 0
    treatment_nodes = 0
    for node_json in parser.docs:
        # assert demographics contents
        if node_json["type"] == "demographic":
            assert "vital_status" in node_json
            assert node_json["vital_status"] == "Dead"
            nodes_found += 1
        elif node_json["type"] == "diagnosis":
            assert node_json["days_to_last_follow_up"] == 4549
            nodes_found += 1
        elif node_json["type"] == "exposure":
            nodes_found += 1
        elif node_json["type"] == "treatment":
            treatment_nodes += 1
    assert nodes_found + treatment_nodes == 5
    assert treatment_nodes == 2
