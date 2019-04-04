from lxml import etree
import pytest

from sheepdog.xml.evaluators import fields


@pytest.mark.parametrize("props, expected",
                         [(dict(path="//admin:file_uuid", nullable="false"), "2940CCCF-533D-4834-A321-2814898DE639"),
                          (dict(path="//admin:file_uuidx", nullable="true"), None),
                          (dict(path="//missing_computation_tag * -1", nullable="true", type="int"), None),
                          (dict(path="//admin:day_of_dcc_upload", nullable="false", type="int"), 22)],
                         ids=["Basic get from XML not nullable string type",
                              "Get Missing Element and nullable",
                              "Handle NaN for int computation",
                              "Basic get from XML not nullable int type"])
def test_basic_evaluator(xml_fixture, props, expected):

    xml = etree.fromstring(xml_fixture)
    nspace = xml.nsmap
    evaluator = fields.BasicEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()


@pytest.mark.parametrize("props, expected",
                         [(dict(path='//shared:days_to_death[not(@procurement_status = "Not Applicable" '
                                     'or @procurement_status = "Not Available")]|//clin_shared:'
                                     'days_to_death[not(@procurement_status = "Not Applicable" '
                                     'or @procurement_status = "Not Available")]',
                                nullable="false", type="int"), 100)])
def test_filter_evaluator(xml_fixture, props, expected):

    xml = etree.fromstring(xml_fixture)
    nspace = xml.nsmap
    evaluator = fields.FilterElementEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()


@pytest.mark.parametrize("props, expected",
                         [(dict(path="//shared:days_to_last_followup|//clin_shared:days_to_last_followup",
                                type="int"), 4549)])
def test_last_follow_up_evaluator(xml_fixture, props, expected):

    xml = etree.fromstring(xml_fixture).xpath("//*[local-name()='patient']")[0]
    nspace = xml.nsmap
    evaluator = fields.LastFollowUpEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()


@pytest.mark.parametrize("props, expected",
                         [(dict(path="./shared:vital_status|./clin_shared:vital_status",
                                evaluator=dict(name="vital_status",
                                               follow_up_path="//shared:days_to_last_followup|"
                                                              "//clin_shared:days_to_last_followup"),
                                type="str.title"), "Dead")])
def test_vital_status_evaluator(xml_fixture, props, expected):

    xml = etree.fromstring(xml_fixture).xpath("//*[local-name()='patient']")[0]
    nspace = xml.nsmap
    evaluator = fields.VitalStatusEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()


@pytest.mark.parametrize("props, expected",
                         [(dict(
                             path=["//clin_shared:radiation_therapy", "//clin_shared:postoperative_rx_tx"],
                             evaluator=dict(
                                 name="treatment_therapy",
                                 new_tumor_event_path="//nte:*[@preferred_name='new_tumor_event_type']",
                                 additional_radiation_path="./nte:additional_radiation_therapy",
                                 additional_pharmaceutical_path="./nte:additional_pharmaceutical_therapy",
                                 allowed_tumor_events=[
                                     "Biochemical evidence of disease",
                                     "Distant Metastasis"
                                ])
                         ), [
                             dict(treatment_type="Radiation Therapy, NOS", treatment_or_therapy="yes"),
                             dict(treatment_type="Pharmaceutical Therapy, NOS", treatment_or_therapy="yes")
                            ])])
def test_treatment_or_therapy_evaluator(xml_radiation_fixture, props, expected):

    xml = etree.fromstring(xml_radiation_fixture).xpath("//*[local-name()='patient']")[0]
    nspace = xml.nsmap
    evaluator = fields.TreatmentTherapyEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()


@pytest.mark.parametrize("props, expected",
                         [(dict(path="//admin:file_uuid/text()", nullable="false",
                                evaluator=dict(
                                    name="unique_value"
                                )), "2940CCCF-533D-4834-A321-2814898DE639"),
                          (dict(path="//clin_shared:days_to_initial_pathologic_diagnosis/text()",
                                evaluator=dict(
                                    name="unique_value"
                                ), type="int"), 0),
                          (dict(path="//clin_shared:unique_value_test/text()",
                                evaluator=dict(
                                    name="unique_value"
                                ), type="str.title", default="not reported"), "Not Reported")])
def test_unique_value_evaluator(xml_fixture, props, expected):

    xml = etree.fromstring(xml_fixture)
    nspace = xml.nsmap
    evaluator = fields.UniqueValueEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()
