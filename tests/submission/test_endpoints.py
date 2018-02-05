import contextlib
import json
import os
import uuid

import boto
import pytest
from flask import g
from moto import mock_s3

from gdcdatamodel import models as md
from sheepdog.transactions.upload import UploadTransaction
from tests.submission.utils import data_fnames, patch_indexclient
from ..auth_mock import Config as auth_conf


#: Do we have a cache case setting and should we do it?
CACHE_CASES = False
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


def test_program_creation_endpoint(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    resp = put_cgci(client, auth=submitter)
    assert resp.status_code == 200, resp.data
    print resp.data
    resp = client.get('/v0/submission/')
    assert resp.json['links'] == ['/v0/submission/CGCI'], resp.json


def test_program_creation_without_admin_token(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    path = '/v0/submission/'
    headers = submitter(path, 'put', 'member')
    data = json.dumps({'name': 'CGCI', 'type': 'program'})
    resp = client.put(path, headers=headers, data=data)
    assert resp.status_code == 403


def test_program_creation_endpoint_for_program_not_supported(
        client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    path = '/v0/submission/abc/'
    resp = client.post(path, headers=submitter(path, 'post'))
    assert resp.status_code == 404


def test_project_creation_endpoint(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_project_creation_without_admin_token(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_put_entity_creation_valid(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_unauthorized_post(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_unauthorized_post_with_incorrect_role(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_check_multiple_samples(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)

    data_payload = []

    with open(os.path.join(DATA_DIR, 'case.json'), 'r') as f:
        case_data = json.loads(f.read())

    data_payload.append(case_data)

    with open(os.path.join(DATA_DIR, 'sample.json'), 'r') as f:
        base_sample = json.loads(f.read())

    data_payload.append(base_sample)
    
    for i in range(0, 10):
        new_sample = dict(base_sample)
        new_sample['parent_samples'] = {
            'submitter_id': new_sample['submitter_id']
        }
        new_sample['sample_type'] = 'DNA'
        new_sample['submitter_id'] = new_sample['submitter_id'] + '_{}'.format(i)

        data_payload.append(new_sample)

    headers = submitter(BLGSP_PATH, 'put')
    resp = client.put(
        BLGSP_PATH, headers=headers, data=json.dumps(data_payload))

    assert resp.status_code == 200
    resp_json = json.loads(resp.data)
    assert resp_json['entity_error_count'] == 0
    #assert resp_json['created_entity_count'] == 1


def test_check_setting_node_open(client, pg_driver, submitter, dictionary_setup):

    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)

    with open(os.path.join(DATA_DIR, 'biospec1.json'), 'r') as f:
        submitted_data = json.loads(f.read())
    for entry in submitted_data:
        if entry['type'] == 'file':
            entry['open_acl'] = True
            break

    headers = submitter(BLGSP_PATH, 'put')
    resp = client.put(
        BLGSP_PATH, headers=headers, data=json.dumps(data_payload))

    assert resp.status_code == 200
    resp_json = json.loads(resp.data)
    assert resp_json['entity_error_count'] == 0
    #assert resp_json['created_entity_count'] == 1


def test_check_setting_node_closed(client, pg_driver, submitter, dictionary_setup):

    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)

    with open(os.path.join(DATA_DIR, 'biospec1.json'), 'r') as f:
        submitted_data = json.loads(f.read())
    for entry in submitted_data:
        if entry['type'] == 'file':
            entry['open_acl'] = False
            break

    headers = submitter(BLGSP_PATH, 'put')
    resp = client.put(
        BLGSP_PATH, headers=headers, data=json.dumps(data_payload))

    assert resp.status_code == 200
    resp_json = json.loads(resp.data)
    assert resp_json['entity_error_count'] == 0
    #assert resp_json['created_entity_count'] == 1


def test_check_setting_disallowed_node_open(client, pg_driver, submitter, dictionary_setup):

    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)

    data_payload = []

    with open(os.path.join(DATA_DIR, 'submitted_unaligned_reads.json'), 'r') as f:
        file_data = json.loads(f.read())
    file_data['open_acl'] = True

    headers = submitter(BLGSP_PATH, 'put')
    resp = client.put(
        BLGSP_PATH, headers=headers, data=json.dumps(data_payload))

    assert resp.status_code == 200
    resp_json = json.loads(resp.data)
    assert resp_json['entity_error_count'] >= 1
    #assert resp_json['created_entity_count'] == 1


def test_put_valid_entity_missing_target(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_put_valid_entity_invalid_type(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_post_example_entities(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_post_example_entities_together(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_dictionary_list_entries(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/CGCI/BLGSP/_dictionary')
    print resp.data
    assert "/v0/submission/CGCI/BLGSP/_dictionary/slide" \
           in json.loads(resp.data)['links']
    assert "/v0/submission/CGCI/BLGSP/_dictionary/case" \
           in json.loads(resp.data)['links']
    assert "/v0/submission/CGCI/BLGSP/_dictionary/aliquot" \
           in json.loads(resp.data)['links']


def test_top_level_dictionary_list_entries(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/_dictionary')
    print resp.data
    assert "/v0/submission/_dictionary/slide" \
           in json.loads(resp.data)['links']
    assert "/v0/submission/_dictionary/case" \
           in json.loads(resp.data)['links']
    assert "/v0/submission/_dictionary/aliquot" \
           in json.loads(resp.data)['links']


def test_dictionary_get_entries(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/CGCI/BLGSP/_dictionary/aliquot')
    assert json.loads(resp.data)['id'] == 'aliquot'


def test_top_level_dictionary_get_entries(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/_dictionary/aliquot')
    assert json.loads(resp.data)['id'] == 'aliquot'


def test_dictionary_get_definitions(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)
    resp = client.get('/v0/submission/CGCI/BLGSP/_dictionary/_definitions')
    assert 'UUID' in resp.json


def test_put_dry_run(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_incorrect_project_error(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_timestamps(client, pg_driver, submitter, dictionary_setup):
    test_post_example_entities(client, pg_driver, submitter, dictionary_setup)
    with pg_driver.session_scope():
        case = pg_driver.nodes(md.Case).first()
        ct = case.created_datetime
        print case.props
        assert ct is not None, case.props


def test_disallow_cross_project_references(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_delete_entity(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_validator_error_types(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_invalid_json(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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

def test_get_entity_by_id(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
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


def test_invalid_file_index(monkeypatch, client, pg_driver, submitter, dictionary_setup):
    """
    Test that submitting an invalid data file doesn't create an index and an
    alias.
    """
    dictionary_setup('s3://test.com')
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


def test_valid_file_index(monkeypatch, client, pg_driver, submitter, dictionary_setup):
    """
    Test that submitting a valid data file creates an index and an alias.
    """

    # Update this dictionary in the patched functions to check that they are
    # called.
    dictionary_setup('s3://test.com')
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

def test_export_entity_by_id(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)
    post_example_entities_together(client, pg_driver, submitter)
    with pg_driver.session_scope():
        case_id = pg_driver.nodes(md.Case).first().node_id
    path = '/v0/submission/CGCI/BLGSP/export/?ids={case_id}'.format(case_id=case_id)
    r = client.get(
        path,
        headers=submitter(path, 'get'))
    assert r.status_code == 200, r.data
    assert r.headers['Content-Disposition'].endswith('tsv')
    path += '&format=json'
    r = client.get(
        path,
        headers=submitter(path, 'get'))

    data = r.json
    assert len(data) == 1
    assert data[0]['id'] == case_id

def test_export_all_node_types(client, pg_driver, submitter, dictionary_setup):
    dictionary_setup('s3://test.com')
    put_cgci_blgsp(client, submitter)
    post_example_entities_together(client, pg_driver, submitter)
    with pg_driver.session_scope() as s:
        case = pg_driver.nodes(md.Case).first()
        new_case = md.Case(str(uuid.uuid4()))
        new_case.props = case.props
        new_case.submitter_id = 'case-2'
        s.add(new_case)
        case_count = pg_driver.nodes(md.Case).count()
    path = '/v0/submission/CGCI/BLGSP/export/?node_label=case'
    r = client.get(
        path,
        headers=submitter(path, 'get'))
    assert r.status_code == 200, r.data
    assert r.headers['Content-Disposition'].endswith('tsv')
    assert len(r.data.strip().split('\n')) == case_count + 1
