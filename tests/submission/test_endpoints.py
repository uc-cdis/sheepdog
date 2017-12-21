import contextlib
import json
import os

import boto
import pytest
from flask import g
from gdcdatamodel import models as md
from gdcdictionary import gdcdictionary
from moto import mock_s3

from sheepdog.transactions.constants import (
    CACHE_CASES,
)

from sheepdog.transactions.upload import UploadTransaction
from tests.submission.utils import data_fnames, patch_indexclient
from ..auth_mock import Config as auth_conf


definitions = gdcdictionary.resolvers['_definitions.yaml'].source
SUBMITTED_STATE = definitions['state']['default']
DEFAULT_FILE_STATE = definitions['file_state']['default']

BLGSP_PATH = '/v0/submission/CGCI/BLGSP/'
BRCA_PATH = '/v0/submission/TCGA/BRCA/'

DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

ADMIN_HEADERS = {"X-Auth-Token": auth_conf.ADMIN_TOKEN}

@contextlib.contextmanager
def s3_conn():
    mock = mock_s3()
    mock.start(reset=False)
    conn = boto.connect_s3()
    yield conn
    bucket = conn.get_bucket('test_submission')
    for part in bucket.list_multipart_uploads():
        part.cancel_upload()
    mock.stop()


def mock_request(f):
    def wrapper(*args, **kwargs):
        mock = mock_s3()
        mock.start(reset=False)
        conn = boto.connect_s3()
        conn.create_bucket('test_submission')

        result = f(*args, **kwargs)
        mock.stop()
        return result

    return wrapper


def put_cgci(client, auth=None, role='admin'):
    path = '/v0/submission'
    headers = auth(path, 'put', role) if auth else None
    data = json.dumps({
        'name': 'CGCI', 'type': 'program',
        'dbgap_accession_number': 'phs000235'
    })
    r = client.put(path, headers=headers, data=data)
    del g.user
    return r


def put_cgci_blgsp(client, auth=None, role='admin'):
    put_cgci(client, auth=auth, role=role)
    path = '/v0/submission/CGCI/'
    headers = auth(path, 'put', role) if auth else None
    data = json.dumps({
        "type": "project",
        "code": "BLGSP",
        "dbgap_accession_number": 'phs000527',
        "name": "Burkitt Lymphoma Genome Sequencing Project",
        "state": "open"
    })
    r = client.put(path, headers=headers, data=data)
    assert r.status_code == 200, r.data
    del g.user
    return r


def put_tcga_brca(client, submitter):
    headers = submitter('/v0/submission/', 'put', 'admin')
    data = json.dumps({
        'name': 'TCGA', 'type': 'program',
        'dbgap_accession_number': 'phs000178'
    })
    r = client.put('/v0/submission/', headers=headers, data=data)
    assert r.status_code == 200, r.data
    headers = submitter('/v0/submission/TCGA/', 'put', 'admin')
    data = json.dumps({
        "type": "project",
        "code": "BRCA",
        "name": "TEST",
        "dbgap_accession_number": "phs000178",
        "state": "open"
    })
    r = client.put('/v0/submission/TCGA/', headers=headers, data=data)
    assert r.status_code == 200, r.data
    del g.user
    return r


def test_program_creation_endpoint(client, pg_driver, submitter):
    resp = put_cgci(client, auth=submitter)
    assert resp.status_code == 200, resp.data
    print resp.data
    resp = client.get('/v0/submission/')
    assert resp.json['links'] == ['/v0/submission/CGCI'], resp.json


def test_program_creation_without_admin_token(client, pg_driver, submitter):
    path = '/v0/submission/'
    headers = submitter(path, 'put', 'member')
    data = json.dumps({'name': 'CGCI', 'type': 'program'})
    resp = client.put(path, headers=headers, data=data)
    assert resp.status_code == 403


def test_program_creation_endpoint_for_program_not_supported(
        client, pg_driver, submitter):
    path = '/v0/submission/abc/'
    resp = client.post(path, headers=submitter(path, 'post'))
    assert resp.status_code == 404


def test_project_creation_endpoint(client, pg_driver, submitter):
    resp = put_cgci_blgsp(client, auth=submitter)
    assert resp.status_code == 200
    resp = client.get('/v0/submission/CGCI/')
    with pg_driver.session_scope():
        assert pg_driver.nodes(md.Project).count() == 1
        n_cgci = (
            pg_driver.nodes(md.Project)
                .path('programs')
                .props(name='CGCI')
                .count()
        )
        assert n_cgci == 1
    assert resp.json['links'] == ['/v0/submission/CGCI/BLGSP'], resp.json


def test_project_creation_without_admin_token(client, pg_driver, submitter):
    put_cgci(client, submitter)
    path = '/v0/submission/CGCI/'
    resp = client.put(
        path, headers=submitter(path, 'put', 'member'), data=json.dumps({
            "type": "project",
            "code": "BLGSP",
            "dbgap_accession_number": "phs000527",
            "name": "Burkitt Lymphoma Genome Sequencing Project",
            "state": "open"}))
    assert resp.status_code == 403


def test_put_entity_creation_valid(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    headers = submitter(BLGSP_PATH, 'put')
    data = json.dumps({
        "type": "experiment",
        "submitter_id": "BLGSP-71-06-00019",
        "projects": {
            "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
        }
    })
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data


def test_unauthorized_post(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    # token for TCGA
    headers = {'X-Auth-Token': auth_conf.SUBMITTER_TOKEN_A}
    data = json.dumps({
        "type": "case",
        "submitter_id": "BLGSP-71-06-00019",
        "projects": {
            "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
        }
    })
    resp = client.post(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 403


def test_unauthorized_post_with_incorrect_role(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    # token only has _member_ role in CGCI
    headers = submitter(BLGSP_PATH, 'post', 'member')
    resp = client.post(
        BLGSP_PATH, headers=headers, data=json.dumps({
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {
                "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
            }}))
    assert resp.status_code == 403


def test_put_valid_entity_missing_target(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)

    with open(os.path.join(DATA_DIR, 'sample.json'), 'r') as f:
        sample = json.loads(f.read())
        sample['cases'] = {"submitter_id": "missing-case"}

    r = client.put(
        BLGSP_PATH,
        headers=submitter(BLGSP_PATH, 'put'),
        data=json.dumps(sample)
    )

    print r.data
    assert r.status_code == 400, r.data
    assert r.status_code == r.json['code']
    assert r.json['entities'][0]['errors'][0]['keys'] == ['cases'], r.json['entities'][0]['errors']
    assert r.json['entities'][0]['errors'][0]['type'] == 'INVALID_LINK'
    assert (
        "[{'project_id': 'CGCI-BLGSP', 'submitter_id': 'missing-case'}]"
        in r.json['entities'][0]['errors'][0]['message']
    )


def test_put_valid_entity_invalid_type(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    r = client.put(
        BLGSP_PATH,
        headers=submitter(BLGSP_PATH, 'put'),
        data=json.dumps([
            {
                "type": "experiment",
                "submitter_id": "BLGSP-71-06-00019",
                "projects": {
                    "code": "BLGSP"
                }
            },
            {
                "type": "case",
                "submitter_id": "BLGSP-71-case-01",
                "experiments": {
                    "submitter_id": 'BLGSP-71-06-00019'
                }
            },
            {
                'type': "demographic",
                'ethnicity': 'not reported',
                'gender': 'male',
                'race': 'asian',
                'submitter_id': 'demographic1',
                'year_of_birth': '1900',
                'year_of_death': 2000,
                'cases': {
                    'submitter_id': 'BLGSP-71-case-01'
                }
            }
        ]))

    print r.json
    assert r.status_code == 400, r.data
    assert r.status_code == r.json['code']
    assert (r.json['entities'][2]['errors'][0]['keys']
            == ['year_of_birth']), r.data
    assert (r.json['entities'][2]['errors'][0]['type']
            == 'INVALID_VALUE'), r.data


def test_post_example_entities(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    path = BLGSP_PATH
    with open(os.path.join(DATA_DIR, 'case.json'), 'r') as f:
        case_sid = json.loads(f.read())['submitter_id']
    for fname in data_fnames:
        with open(os.path.join(DATA_DIR, fname), 'r') as f:
            resp = client.post(
                path, headers=submitter(path, 'post'), data=f.read()
            )
            assert resp.status_code == 201, resp.data
            if CACHE_CASES and fname not in ['experiment.json', 'case.json']:
                case = resp.json['entities'][0]['related_cases'][0]
                assert (case['submitter_id'] == case_sid), (fname, resp.data)


def post_example_entities_together(
        client, pg_driver, submitter, data_fnames2=None):
    if data_fnames2 is None:
        data_fnames2 =  data_fnames
    path = BLGSP_PATH
    data = []
    for fname in data_fnames2:
        with open(os.path.join(DATA_DIR, fname), 'r') as f:
            data.append(json.loads(f.read()))
    return client.post(path, headers=submitter(path, 'post'), data=json.dumps(data))


def put_example_entities_together(client, pg_driver, submitter):
    path = BLGSP_PATH
    data = []
    for fname in data_fnames:
        with open(os.path.join(DATA_DIR, fname), 'r') as f:
            data.append(json.loads(f.read()))
    return client.put(path, headers=submitter(path, 'put'), data=json.dumps(data))


def test_post_example_entities_together(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    with open(os.path.join(DATA_DIR, 'case.json'), 'r') as f:
        case_sid = json.loads(f.read())['submitter_id']
    resp = post_example_entities_together(client, pg_driver, submitter)
    print resp.data
    assert resp.status_code == 201, resp.data
    if CACHE_CASES:
        assert resp.json['entities'][2]['related_cases'][0]['submitter_id'] \
               == case_sid, resp.data

@pytest.mark.skipif(not CACHE_CASES, reason="This dictionary does not cache cases")
def test_related_cases(client, pg_driver, submitter):
    assert put_cgci_blgsp(client, submitter).status_code == 200
    with open(os.path.join(DATA_DIR, 'case.json'), 'r') as f:
        case_id = json.loads(f.read())['submitter_id']

    resp = post_example_entities_together(client, pg_driver, submitter)
    assert resp.json["cases_related_to_created_entities_count"] == 1, resp.data
    assert resp.json["cases_related_to_updated_entities_count"] == 0, resp.data
    for e in resp.json['entities']:
        for c in e['related_cases']:
            assert c['submitter_id'] == case_id, resp.data
    resp = put_example_entities_together(client, pg_driver, submitter)
    assert resp.json["cases_related_to_created_entities_count"] == 0, resp.data
    assert resp.json["cases_related_to_updated_entities_count"] == 1, resp.data


def test_dictionary_list_entries(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/CGCI/BLGSP/_dictionary')
    print resp.data
    assert "/v0/submission/CGCI/BLGSP/_dictionary/slide" \
           in json.loads(resp.data)['links']
    assert "/v0/submission/CGCI/BLGSP/_dictionary/case" \
           in json.loads(resp.data)['links']
    assert "/v0/submission/CGCI/BLGSP/_dictionary/aliquot" \
           in json.loads(resp.data)['links']


def test_top_level_dictionary_list_entries(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/_dictionary')
    print resp.data
    assert "/v0/submission/_dictionary/slide" \
           in json.loads(resp.data)['links']
    assert "/v0/submission/_dictionary/case" \
           in json.loads(resp.data)['links']
    assert "/v0/submission/_dictionary/aliquot" \
           in json.loads(resp.data)['links']

def test_dictionary_get_entries(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/CGCI/BLGSP/_dictionary/aliquot')
    assert json.loads(resp.data)['id'] == 'aliquot'


def test_top_level_dictionary_get_entries(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/_dictionary/aliquot')
    assert json.loads(resp.data)['id'] == 'aliquot'


def test_dictionary_get_definitions(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/CGCI/BLGSP/_dictionary/_definitions')
    assert 'UUID' in resp.json


def test_put_dry_run(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    path = '/v0/submission/CGCI/BLGSP/_dry_run/'
    resp = client.put(
        path,
        headers=submitter(path, 'put'),
        data=json.dumps({
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {
                "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
            }}))
    assert resp.status_code == 200, resp.data
    resp_json = json.loads(resp.data)
    assert resp_json['entity_error_count'] == 0
    assert resp_json['created_entity_count'] == 1
    with pg_driver.session_scope():
        assert not pg_driver.nodes(md.Experiment).first()


def test_incorrect_project_error(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    put_tcga_brca(client, submitter)
    resp = client.put(
        BLGSP_PATH,
        headers=submitter(BLGSP_PATH, 'put'),
        data=json.dumps({
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {
                "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
            }}))
    resp = client.put(
        BRCA_PATH,
        headers=submitter(BRCA_PATH, 'put'),
        data=json.dumps({
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {
                "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
            }}))
    resp_json = json.loads(resp.data)
    assert resp.status_code == 400
    assert resp_json['code'] == 400
    assert resp_json['entity_error_count'] == 1
    assert resp_json['created_entity_count'] == 0
    assert (resp_json['entities'][0]['errors'][0]['type']
            == 'INVALID_PERMISSIONS')


def test_timestamps(client, pg_driver, submitter):
    test_post_example_entities(client, pg_driver, submitter)
    with pg_driver.session_scope():
        case = pg_driver.nodes(md.Case).first()
        ct = case.created_datetime
        print case.props
        assert ct is not None, case.props


def test_disallow_cross_project_references(client, pg_driver, submitter):
    put_tcga_brca(client, submitter)
    put_cgci_blgsp(client, submitter)
    data = {
        "progression_or_recurrence": "unknown",
        "classification_of_tumor": "other",
        "last_known_disease_status": "Unknown tumor status",
        "tumor_grade": "",
        "tissue_or_organ_of_origin": "c34.3",
        "days_to_last_follow_up": -1.0,
        "primary_diagnosis": "c34.3",
        "submitter_id": "E9EDB78B-6897-4205-B9AA-0CEF8AAB5A1F_diagnosis",
        "site_of_resection_or_biopsy": "c34.3",
        "tumor_stage": "stage iiia",
        "days_to_birth": -17238.0,
        "age_at_diagnosis": 47,
        "vital_status": "dead",
        "morphology": "8255/3",
        "cases": {
            "submitter_id": "BLGSP-71-06-00019"
        },
        "type": "diagnosis",
        "prior_malignancy": "no",
        "days_to_recurrence": -1,
        "days_to_last_known_disease_status": -1
    }
    resp = client.put(
        BRCA_PATH,
        headers=submitter(BRCA_PATH, 'put'),
        data=json.dumps(data))
    assert resp.status_code == 400, resp.data


def test_delete_entity(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    resp = client.put(
        BLGSP_PATH,
        headers=submitter(BLGSP_PATH, 'put'),
        data=json.dumps({
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {
                "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
            }}))
    assert resp.status_code == 200, resp.data
    did = resp.json['entities'][0]['id']
    path = BLGSP_PATH + 'entities/' + did
    resp = client.delete(path, headers=submitter(path, 'delete'))
    assert resp.status_code == 200, resp.data


def test_catch_internal_errors(monkeypatch, client, pg_driver, submitter):
    """
    Monkey patch an essential function to just raise an error and assert that
    this error is caught and recorded as a transactional_error.
    """
    put_cgci_blgsp(client, submitter)

    def just_raise_exception(self):
        raise Exception('test')

    monkeypatch.setattr(UploadTransaction, 'pre_validate', just_raise_exception)
    try:
        r = put_example_entities_together(client, pg_driver, submitter)
        assert len(r.json['transactional_errors']) == 1, r.data
    except:
        raise


# def create_file(app, client, submitter, pg_driver, state=DEFAULT_FILE_STATE):
#     put_cgci_blgsp(client, submitter)
#     doc = app.signpost.create()
#     with pg_driver.session_scope() as s:
#         f = md.File(doc.did)
#         f.file_state = state
#         f.project_id = 'CGCI-BLGSP'
#         s.add(f)
#     return doc


# def test_file_upload(app, client, pg_driver, submitter):
#     doc = create_file(app, client, pg_driver, submitter)
#     with patch.multiple("gdcapi.resources.submission.files",
#                make_s3_request=mock_request(make_s3_request),
#                get_s3_hosts=lambda *args: [None]):
#         path = "/submission/CGCI/BLGSP/files/{}".format(doc.did)
#         r = client.put(path,
#                    data="test",
#                    headers=submitter(path, 'put'))
#         print r.data
#     with s3_conn() as conn:
#         bucket = conn.get_bucket('test_submission')
#         key = bucket.get_key("CGCI-BLGSP/"+doc.did)
#         assert key.read() == "test"

# def test_file_upload_for_error_file(app, client, pg_driver, submitter):
#     doc = create_file(app, client, pg_driver, 'error')
#     with patch.multiple("gdcapi.resources.submission.files",
#                make_s3_request=mock_request(make_s3_request),
#                get_s3_hosts=lambda *args: [None]):
#         path = "/submission/CGCI/BLGSP/files/{}".format(doc.did)
#         r = client.put(path,
#                    data="test",
#                    headers=submitter(path, 'put'))
#     assert r.status_code == 200


# def test_file_upload_for_uploaded_file(app, client, pg_driver, submitter):
#     doc = create_file(app, client, pg_driver, 'uploaded')
#     with patch.multiple("gdcapi.resources.submission.files",
#                make_s3_request=mock_request(make_s3_request),
#                get_s3_hosts=lambda *args: [None]):
#         path = "/submission/CGCI/BLGSP/files/{}".format(doc.did)
#         r = client.put(path,
#                    data="test",
#                    headers=submitter(path, 'put'))
#     assert r.status_code == 400

# def test_file_multipart_initiate(app, client, pg_driver, submitter):
#     doc = create_file(app, client, pg_driver, submitter)
#     with patch.multiple("gdcapi.resources.submission.files",
#                make_s3_request=mock_request(make_s3_request),
#                get_s3_hosts=lambda *args: [None]):
#         path = "/submission/CGCI/BLGSP/files/{}?uploads".format(doc.did)
#         res = client.post(
#             path,
#             headers=submitter(path, 'put'))

#         assert res.status_code == 200
#         result = etree.fromstring(res.data)
#         namespace = result.nsmap[None]
#         assert result.find('{%s}UploadId' % namespace).text
#         assert result.find('{%s}Key' % namespace).text == \
#             "CGCI-BLGSP/"+doc.did


# def test_file_multipart_upload_part(app, client, pg_driver, submitter):
#     doc = create_file(app, client, pg_driver, submitter)
#     with patch.multiple("gdcapi.resources.submission.files",
#                make_s3_request=mock_request(make_s3_request),
#                get_s3_hosts=lambda *args: [None]):
#         path = "/submission/CGCI/BLGSP/files/{}?uploads".format(doc.did)
#         res = client.post(
#             path,
#             headers=submitter(path, 'post'))

#         result = etree.fromstring(res.data)
#         namespace = result.nsmap[None]
#         upload_id = result.find('{%s}UploadId' % namespace).text
#         path = (
#             "/submission/CGCI/BLGSP/files/{}?uploadId={}&partNumber=1"
#             .format(doc.did, upload_id))
#         res = client.put(
#             path,
#             headers=submitter(path, 'put'))
#         assert res.status_code == 200
#     with s3_conn() as conn:
#         bucket = conn.get_bucket('test_submission')
#         parts = [part.id for part in bucket.list_multipart_uploads()]
#         assert upload_id  in parts


# def test_file_multipart_complete(app, client, pg_driver, submitter):
#     doc = create_file(app, client, pg_driver, submitter)
#     with patch.multiple("gdcapi.resources.submission.files",
#                make_s3_request=mock_request(make_s3_request),
#                get_s3_hosts=lambda *args: [None]):
#         path = "/submission/CGCI/BLGSP/files/{}?uploads".format(doc.did)
#         res = client.post(
#             path,
#             headers=submitter(path, 'post'))

#         result = etree.fromstring(res.data)
#         namespace = result.nsmap[None]
#         upload_id = result.find('{%s}UploadId' % namespace).text
#         path = (
#             "/submission/CGCI/BLGSP/files/{}?uploadId={}&partNumber=1"
#             .format(doc.did, upload_id))
#         res = client.put(
#             path,
#             data="test",
#             headers=submitter(path, 'put'))
#         all_parts = etree.Element("CompleteMultipartUpload")
#         part = etree.SubElement(all_parts, "Part")
#         part_number = etree.SubElement(part, "PartNumber")
#         part_number.text = "1"
#         etag = etree.SubElement(part, "ETag")
#         etag.text = res.headers['ETag']
#         path = (
#             "/submission/CGCI/BLGSP/files/{}?uploadId={}"
#             .format(doc.did, upload_id))
#         res = client.post(
#             path,
#             data=etree.tostring(all_parts),
#             headers=submitter(path, 'put'))
#         assert res.status_code == 200
#     with s3_conn() as conn:
#         bucket = conn.get_bucket('test_submission')
#         key = bucket.get_key("CGCI-BLGSP/"+doc.did)
#         assert key.read() == "test"


def test_validator_error_types(client, pg_driver, submitter):
    assert put_cgci_blgsp(client, submitter).status_code == 200
    assert put_example_entities_together(client, pg_driver, submitter).status_code == 200

    r = client.put(
        BLGSP_PATH,
        headers=submitter(BLGSP_PATH, 'put'),
        data=json.dumps({
            "type": "sample",
            "cases": {
                "submitter_id": "BLGSP-71-06-00019"
            },
            "is_ffpe": "maybe",
            "sample_type": "Blood Derived Normal",
            "submitter_id": "BLGSP-71-06-00019",
            "longest_dimension": -1.0
        }))
    errors = {
        e['keys'][0]: e['type']
        for e in r.json['entities'][0]['errors']
    }
    assert r.status_code == 400, r.data
    assert errors['is_ffpe'] == 'INVALID_VALUE'
    assert errors['longest_dimension'] == 'INVALID_VALUE'


def test_invalid_json(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    resp = client.put(
        BLGSP_PATH,
        headers=submitter(BLGSP_PATH, 'put'),
        data="""{
    "key1": "valid value",
    "key2": not a string,
}""")
    print resp.data
    assert resp.status_code == 400
    assert 'Expecting value' in resp.json['message']


# def test_invalid_dbgap_id(client, pg_driver, submitter):
#     put_cgci_blgsp(client, submitter)
#     resp = client.put(
#         BLGSP_PATH,
#         headers=submitter(BLGSP_PATH, 'put'),
#         data=json.dumps({
#             "type": "case",
#             "submitter_id": "FAKE-ID",
#             "projects": {
#                 "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
#             }
#         }))

#     print resp.data
#     assert resp.status_code == 400
#     assert resp.json['entities'][0]['errors'] == [
#         {
#             "keys": [
#                 "submitter_id"
#             ],
#             "message": "Case submitter_id 'FAKE-ID' not found in dbGaP.",
#             "type": "NOT_FOUND"
#         }
#     ]


# def test_dbgap_bypassed_cases(client, pg_driver, submitter):
#     put_cgci_blgsp(client, submitter)
#     with pg_driver.session_scope():
#         project = pg_driver.nodes(md.Project).props(code='BLGSP').one()
#         project.sysan['dbgap_bypassed_cases'] = [
#             'submitter_id_1',
#         ]

#     case1 = {
#         "type": "case",
#         "submitter_id": "submitter_id_1",
#         "projects": {
#             "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
#         }
#     }

#     case2 = {
#         "type": "case",
#         "submitter_id": "submitter_id_2",
#         "projects": {
#             "id": "daa208a7-f57a-562c-a04a-7a7c77542c98"
#         }
#     }

#     # Verify that case2 is still forbidden
#     resp = client.put(
#         BLGSP_PATH,
#         headers=submitter(BLGSP_PATH, 'put'),
#         data=json.dumps(case2))
#     assert resp.status_code == 400
#     assert resp.json['entities'][0]['errors'] == [
#         {
#             "keys": [
#                 "submitter_id"
#             ],
#             "message": "Case submitter_id 'submitter_id_2' not found in dbGaP.",
#             "type": "NOT_FOUND"
#         }
#     ]

#     # Verify that case1 is allowed as an exception
#     resp = client.put(
#         BLGSP_PATH,
#         headers=submitter(BLGSP_PATH, 'put'),
#         data=json.dumps(case1))
#     assert resp.status_code == 200


# def test_submitted_unaligned_reads_default_state(client, pg_driver, submitter):
#     put_cgci_blgsp(client, submitter)
#     put_example_entities_together(client, pg_driver, submitter)
#     put_entity_from_file(client, 'read_group.json', submitter)
#     put_entity_from_file(client, 'submitted_unaligned_reads.json', submitter)

#     with pg_driver.session_scope():
#         sf = pg_driver.nodes(md.SubmittedUnalignedReads).one()
#         assert sf.state == 'validated'
#         assert sf.file_state == 'registered'

#     put_entity_from_file(client, 'submitted_unaligned_reads.json', submitter)


# def test_submitted_unaligned_reads_default_state_update(client, pg_driver, submitter):
#     put_cgci_blgsp(client, submitter)
#     put_example_entities_together(client, pg_driver, submitter)
#     put_entity_from_file(client, 'read_group.json')
#     put_entity_from_file(client, 'submitted_unaligned_reads.json')

#     with pg_driver.session_scope() as s:
#         sf = pg_driver.nodes(md.SubmittedUnalignedReads).one()
#         sf.state = 'submitted'
#         sf.file_state = 'uploading'
#         s.merge(sf)

#     put_entity_from_file(client, 'submitted_unaligned_reads.json')


# def test_submitted_unaligned_reads_disallowed_update(client, pg_driver, submitter):
#     put_cgci_blgsp(client, submitter)
#     put_example_entities_together(client, pg_driver, submitter)
#     put_entity_from_file(client, 'read_group.json')
#     put_entity_from_file(client, 'submitted_unaligned_reads.json')

#     allowed_states = [
#         'registered',
#         'uploading',
#         'uploaded',
#         'validating',
#     ]

#     disallowed_states = [
#         'validated',
#         'submitted',
#         'processing',
#         'processed',
#     ]

#     for state in allowed_states:
#         with pg_driver.session_scope():
#             pg_driver.nodes(md.SubmittedUnalignedReads).one().file_state = state
#         put_entity_from_file(client, 'submitted_unaligned_reads.json')

#     for state in disallowed_states:
#         with pg_driver.session_scope():
#             pg_driver.nodes(md.SubmittedUnalignedReads).one().file_state = state

#         r = put_entity_from_file(client, 'submitted_unaligned_reads.json', validate=False)
#         assert r.status_code == 400
#         assert r.json['entities'][0]['errors'][0]['keys'] == ['file_state']


# def test_is_file(client, pg_driver, submitter):
#     """Test logic about what counts as a data_file
#     """

#     assert UploadEntity.is_file(md.File('1'))
#     assert not UploadEntity.is_file(md.Case('1'))
#     assert not UploadEntity.is_file(md.Sample('1'))
#     assert UploadEntity.is_file(md.Archive('1'))


# def test_is_updatable_file(client, pg_driver, submitter):
#     """Test logic about what is a submittable data_file
#     """

#     allowed_states = [
#         'registered',
#         'uploading',
#         'uploaded',
#         'validating',
#     ]

#     disallowed_states = [
#         'validated',
#         'submitted',
#         'processing',
#         'processed',
#     ]

#     assert not UploadEntity.is_updatable_file(md.Case('case1'))

#     for state in allowed_states:
#         assert UploadEntity.is_updatable_file(
#             md.File('file1', file_state=state)
#         )

#     for state in disallowed_states:
#         assert not UploadEntity.is_updatable_file(
#             md.File('file1', file_state=state)
#         )


def test_get_entity_by_id(client, pg_driver, submitter):
    put_cgci_blgsp(client, submitter)
    post_example_entities_together(client, pg_driver, submitter)
    with pg_driver.session_scope():
        case_id = pg_driver.nodes(md.Case).first().node_id
    path = '/v0/submission/CGCI/BLGSP/entities/{case_id}'.format(case_id=case_id)
    r = client.get(
        path,
        headers=submitter(path, 'get'))
    assert r.status_code == 200, r.data
    assert r.json['entities'][0]['properties']['id'] == case_id, r.data


def test_invalid_file_index(monkeypatch, client, pg_driver, submitter):
    """
    Test that submitting an invalid data file doesn't create an index and an
    alias.
    """

    def fail_index_test(_):
        raise AssertionError('IndexClient tried to create index or alias')

    # Since the IndexClient should never be called to register anything if the
    # file is invalid, change the ``create`` and ``create_alias`` methods to
    # raise an error.
    monkeypatch.setattr(
        UploadTransaction, 'signpost.create', fail_index_test, raising=False
    )
    monkeypatch.setattr(
        UploadTransaction, 'signpost.create_alias', fail_index_test,
        raising=False
    )
    # Attempt to post the invalid entities.
    put_cgci_blgsp(client, auth=submitter)
    test_fnames = (
        data_fnames
        + ['read_group.json', 'submitted_unaligned_reads_invalid.json']
    )
    resp = post_example_entities_together(
        client, pg_driver, submitter, data_fnames2=test_fnames
    )
    print(resp)


def test_valid_file_index(monkeypatch, client, pg_driver, submitter):
    """
    Test that submitting a valid data file creates an index and an alias.
    """

    # Update this dictionary in the patched functions to check that they are
    # called.
    called = patch_indexclient(monkeypatch)

    # Attempt to post the valid entities.
    put_cgci_blgsp(client, auth=submitter)
    test_fnames = (
        data_fnames
        + ['read_group.json', 'submitted_unaligned_reads.json']
    )
    resp = post_example_entities_together(
        client, pg_driver, submitter, data_fnames2=test_fnames
    )
    print(resp)

    assert called['create']
    assert called['create_alias']
