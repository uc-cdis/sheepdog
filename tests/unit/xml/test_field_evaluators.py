import pytest

from sheepdog.xml.evaluators.fields import BasicEvaluator, FilterElementEvaluator, LastFollowUpEvaluator, \
    TreatmentTherapyEvaluator, VitalStatusEvaluator


@pytest.mark.parametrize("props, expected",
                         [(dict(path="//admin:file_uuid", nullable="false"), "2940CCCF-533D-4834-A321-2814898DE639"),
                          (dict(path="//admin:file_uuidx", nullable="true"), None),
                          (dict(path="//admin:day_of_dcc_upload", nullable="false", type="int"), 22)])
def test_basic_evaluator(xml_fixture, props, expected):

    xml = xml_fixture
    nspace = xml.nsmap
    evaluator = BasicEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()


@pytest.mark.parametrize("props, expected",
                         [(dict(path='//shared:days_to_death[not(@procurement_status = "Not Applicable" '
                                     'or @procurement_status = "Not Available")]|//clin_shared:'
                                     'days_to_death[not(@procurement_status = "Not Applicable" '
                                     'or @procurement_status = "Not Available")]',
                                nullable="false", type="int"), 100)])
def test_filter_evaluator(xml_fixture, props, expected):

    xml = xml_fixture
    nspace = xml.nsmap
    evaluator = FilterElementEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()


@pytest.mark.parametrize("props, expected",
                         [(dict(path="//shared:days_to_last_followup|//clin_shared:days_to_last_followup",
                                type="int"), 4549)])
def test_last_follow_up_evaluator(xml_fixture, props, expected):

    xml = xml_fixture.xpath("//*[local-name()='patient']")[0]
    nspace = xml.nsmap
    evaluator = LastFollowUpEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()


@pytest.mark.parametrize("props, expected",
                         [(dict(path="./shared:vital_status|./clin_shared:vital_status",
                                evaluator=dict(name="vital_status",
                                               follow_up_path="//shared:days_to_last_followup|"
                                                              "//clin_shared:days_to_last_followup"),
                                type="str.title"), "Dead")])
def test_vital_status_evaluator(xml_fixture, props, expected):

    xml = xml_fixture.xpath("//*[local-name()='patient']")[0]
    nspace = xml.nsmap
    evaluator = VitalStatusEvaluator(xml, nspace, props)
    assert expected == evaluator.evaluate()
