# -*- coding: utf-8 -*-

# this is for interpreting fixtures as parameters that don't do anything
# pylint: disable=unused-argument
# pylint: disable=superfluous-parens
# pylint: disable=no-member
import contextlib
import csv
import json
import os
import uuid
from io import StringIO

import boto
from datamodelutils import models as md
from flask import g
from moto import mock_s3
from sqlalchemy.exc import IntegrityError, OperationalError

from sheepdog.globals import ROLES
from sheepdog.transactions.upload import UploadTransaction
from sheepdog.utils import get_external_proxies
from sheepdog.utils.transforms import TSVToJSONConverter
from tests.integration.datadict.submission.utils import (
    data_fnames,
    extended_data_fnames,
)

BLGSP_PATH = "/v0/submission/CGCI/BLGSP/"
BRCA_PATH = "/v0/submission/TCGA/BRCA/"

DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")


@contextlib.contextmanager
def s3_conn():
    mock = mock_s3()
    mock.start(reset=False)
    conn = boto.connect_s3()
    yield conn
    bucket = conn.get_bucket("test_submission")
    for part in bucket.list_multipart_uploads():
        part.cancel_upload()
    mock.stop()


def mock_request(f):
    def wrapper(*args, **kwargs):
        mock = mock_s3()
        mock.start(reset=False)
        conn = boto.connect_s3()
        conn.create_bucket("test_submission")

        result = f(*args, **kwargs)
        mock.stop()
        return result

    return wrapper


def put_cgci(client, auth=None):
    path = "/v0/submission"
    headers = auth
    data = json.dumps(
        {
            "name": "CGCI",
            "type": "program",
            "dbgap_accession_number": "phs000235",
        }  # noqa: E501
    )
    r = client.put(path, headers=headers, data=data)
    return r


def put_cgci_blgsp(client, auth=None):
    r = put_cgci(client, auth=auth)
    assert r.status_code == 200, r.data

    path = "/v0/submission/CGCI/"
    headers = auth
    data = json.dumps(
        {
            "type": "project",
            "code": "BLGSP",
            "dbgap_accession_number": "phs000527",
            "name": "Burkitt Lymphoma Genome Sequencing Project",
            "state": "open",
        }
    )
    r = client.put(path, headers=headers, data=data)
    assert r.status_code == 200, r.data
    del g.user
    return r


def put_tcga_brca(client, submitter):
    headers = submitter
    data = json.dumps(
        {
            "name": "TCGA",
            "type": "program",
            "dbgap_accession_number": "phs000178",
        }  # noqa: E501
    )
    r = client.put("/v0/submission/", headers=headers, data=data)
    assert r.status_code == 200, r.data
    headers = submitter
    data = json.dumps(
        {
            "type": "project",
            "code": "BRCA",
            "name": "TEST",
            "dbgap_accession_number": "phs000178",
            "state": "open",
        }
    )
    r = client.put("/v0/submission/TCGA/", headers=headers, data=data)
    assert r.status_code == 200, r.data
    del g.user
    return r


def add_and_get_new_experimental_metadata_count(pg_driver):
    with pg_driver.session_scope() as s:
        experimental_metadata = pg_driver.nodes(
            md.ExperimentalMetadata
        ).first()  # noqa: E501
        new_experimental_metadata = md.ExperimentalMetadata(str(uuid.uuid4()))
        new_experimental_metadata.props = experimental_metadata.props
        new_experimental_metadata.submitter_id = "case-2"
        s.add(new_experimental_metadata)
        experimental_metadata_count = pg_driver.nodes(
            md.ExperimentalMetadata
        ).count()  # noqa: E501
    return experimental_metadata_count


def test_program_creation_endpoint(client, pg_driver, submitter):
    # Does not test authz.
    resp = put_cgci(client, auth=submitter)
    assert resp.status_code == 200, resp.data
    print(resp.data)
    resp = client.get("/v0/submission/")
    condition_to_check = "/v0/submission/CGCI" in resp.json["links"] and resp.json
    assert condition_to_check, resp.json


def test_program_creation_unauthorized(
    client, pg_driver, submitter, mock_arborist_requests
):
    # Just checks that this is guarded with an Arborist auth request.
    # (Does not check that the auth request is for the Sheepdog admin policy.)
    mock_arborist_requests(authorized=False)
    path = "/v0/submission/"
    headers = submitter
    data = json.dumps({"name": "CGCI", "type": "program"})
    resp = client.put(path, headers=headers, data=data)
    assert resp.status_code == 403


def test_program_creation_endpoint_for_program_not_supported(
    client, pg_driver, submitter
):
    path = "/v0/submission/abc/"
    resp = client.post(path, headers=submitter)
    assert resp.status_code == 404


def test_project_creation_endpoint(client, pg_driver, submitter):
    # Does not test authz.
    resp = put_cgci_blgsp(client, auth=submitter)
    assert resp.status_code == 200
    resp = client.get("/v0/submission/CGCI/")
    with pg_driver.session_scope():
        assert pg_driver.nodes(md.Project).count() == 1
        n_cgci = (
            pg_driver.nodes(md.Project)
            .path("programs")
            .props(name="CGCI")
            .count()  # noqa: E501
        )
        assert n_cgci == 1
    assert resp.json["links"] == ["/v0/submission/CGCI/BLGSP"], resp.json


def test_project_creation_unauthorized(
    client, pg_driver, submitter, mock_arborist_requests
):
    # Just checks that this is guarded with an Arborist auth request.
    # (Does not check that the auth request is for the Sheepdog admin policy.)
    put_cgci(client, submitter)
    path = "/v0/submission/CGCI/"

    mock_arborist_requests(authorized=False)
    resp = client.put(
        path,
        headers=submitter,
        data=json.dumps(
            {
                "type": "project",
                "code": "BLGSP",
                "dbgap_accession_number": "phs000527",
                "name": "Burkitt Lymphoma Genome Sequencing Project",
                "state": "open",
            }
        ),
    )
    assert resp.status_code == 403


def test_put_entity_creation_valid(client, pg_driver, cgci_blgsp, submitter):
    headers = submitter
    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
        }
    )
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data


def test_unauthenticated_post(client, pg_driver, cgci_blgsp, submitter):
    # send garbage token
    headers = {"Authorization": "test"}
    data = json.dumps(
        {
            "type": "case",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
        }
    )
    resp = client.post(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 401


def test_unauthorized_post(
    client, pg_driver, cgci_blgsp, submitter, mock_arborist_requests
):
    headers = submitter
    mock_arborist_requests(authorized=False)
    resp = client.post(
        BLGSP_PATH,
        headers=headers,
        data=json.dumps(
            {
                "type": "experiment",
                "submitter_id": "BLGSP-71-06-00019",
                "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            }
        ),
    )
    assert resp.status_code == 403


def test_put_valid_entity_missing_target(
    client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    with open(os.path.join(DATA_DIR, "sample.json"), "r") as f:
        sample = json.loads(f.read())
        sample["cases"] = {"submitter_id": "missing-case"}

    r = client.put(BLGSP_PATH, headers=submitter, data=json.dumps(sample))

    print(r.data)
    assert r.status_code == 400, r.data
    assert r.status_code == r.json["code"]
    assert r.json["entities"][0]["errors"][0]["keys"] == ["cases"], r.json[
        "entities"
    ][  # noqa: E501
        0
    ][
        "errors"
    ]
    assert r.json["entities"][0]["errors"][0]["type"] == "INVALID_LINK"
    assert (
        "[{'project_id': 'CGCI-BLGSP', 'submitter_id': 'missing-case'}]"
        in r.json["entities"][0]["errors"][0]["message"]
    )


def test_put_valid_entity_invalid_type(
    client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    r = client.put(
        BLGSP_PATH,
        headers=submitter,
        data=json.dumps(
            [
                {
                    "type": "experiment",
                    "submitter_id": "BLGSP-71-06-00019",
                    "projects": {"code": "BLGSP"},
                },
                {
                    "type": "case",
                    "submitter_id": "BLGSP-71-case-01",
                    "experiments": {"submitter_id": "BLGSP-71-06-00019"},
                },
                {
                    "type": "demographic",
                    "ethnicity": "not reported",
                    "gender": "male",
                    "race": "asian",
                    "submitter_id": "demographic1",
                    "year_of_birth": "1900",
                    "year_of_death": 2000,
                    "cases": {"submitter_id": "BLGSP-71-case-01"},
                },
            ]
        ),
    )

    print(r.json)
    assert r.status_code == 400, r.data
    assert r.status_code == r.json["code"]
    assert r.json["entities"][2]["errors"][0]["keys"] == [
        "year_of_birth"
    ], r.data  # noqa: E501
    assert (
        r.json["entities"][2]["errors"][0]["type"] == "INVALID_VALUE"
    ), r.data  # noqa: E501


def test_post_example_entities(client, pg_driver, cgci_blgsp, submitter):
    path = BLGSP_PATH
    for fname in data_fnames:
        with open(os.path.join(DATA_DIR, fname), "r") as f:
            data = json.loads(f.read())
            resp = client.post(path, headers=submitter, data=json.dumps(data))
            resp_data = json.loads(resp.data)
            # could already exist in the DB.
            condition_to_check = (resp.status_code == 201 and resp.data) or (
                resp.status_code == 400
                and "already exists in the DB"
                in resp_data["entities"][0]["errors"][0]["message"]
            )
            assert condition_to_check, resp.data


def post_example_entities_together(client, submitter, data_fnames2=None):
    if not data_fnames2:
        data_fnames2 = data_fnames
    path = BLGSP_PATH
    data = []
    for fname in data_fnames2:
        with open(os.path.join(DATA_DIR, fname), "r") as f:
            data.append(json.loads(f.read()))
    resp = client.post(path, headers=submitter, data=json.dumps(data))
    return resp


def put_example_entities_together(client, headers):
    path = BLGSP_PATH
    data = []
    for fname in data_fnames:
        with open(os.path.join(DATA_DIR, fname), "r") as f:
            data.append(json.loads(f.read()))
    return client.put(path, headers=headers, data=json.dumps(data))


def test_post_example_entities_together(
    client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    with open(os.path.join(DATA_DIR, "case.json"), "r") as f:
        case_sid = json.loads(f.read())["submitter_id"]
        print(case_sid)
    resp = post_example_entities_together(client, submitter)
    print(resp.data)
    resp_data = json.loads(resp.data)
    # could already exist in the DB.
    condition_to_check = (resp.status_code == 201 and resp.data) or (
        resp.status_code == 400
        and "already exists in the DB"
        in resp_data["entities"][0]["errors"][0]["message"]
    )
    assert condition_to_check, resp.data


def test_dictionary_list_entries(client, pg_driver, cgci_blgsp, submitter):
    resp = client.get("/v0/submission/CGCI/BLGSP/_dictionary")
    print(resp.data)
    assert (
        "/v0/submission/CGCI/BLGSP/_dictionary/slide"
        in json.loads(resp.data)["links"]  # noqa: E501
    )
    assert (
        "/v0/submission/CGCI/BLGSP/_dictionary/case"
        in json.loads(resp.data)["links"]  # noqa: E501
    )
    assert (
        "/v0/submission/CGCI/BLGSP/_dictionary/aliquot"
        in json.loads(resp.data)["links"]
    )


def test_top_level_dictionary_list_entries(
    client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    resp = client.get("/v0/submission/_dictionary")
    print(resp.data)
    assert "/v0/submission/_dictionary/slide" in json.loads(resp.data)["links"]
    assert "/v0/submission/_dictionary/case" in json.loads(resp.data)["links"]
    assert (
        "/v0/submission/_dictionary/aliquot" in json.loads(resp.data)["links"]
    )  # noqa: E501


def test_dictionary_get_entries(client, pg_driver, cgci_blgsp, submitter):
    resp = client.get("/v0/submission/CGCI/BLGSP/_dictionary/aliquot")
    assert json.loads(resp.data)["id"] == "aliquot"


def test_top_level_dictionary_get_entries(
    client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    resp = client.get("/v0/submission/_dictionary/aliquot")
    assert json.loads(resp.data)["id"] == "aliquot"


def test_dictionary_get_definitions(client, pg_driver, cgci_blgsp, submitter):
    resp = client.get("/v0/submission/CGCI/BLGSP/_dictionary/_definitions")
    assert "UUID" in resp.json


def test_put_dry_run(client, pg_driver, cgci_blgsp, submitter):
    path = "/v0/submission/CGCI/BLGSP/_dry_run/"
    resp = client.put(
        path,
        headers=submitter,
        data=json.dumps(
            {
                "type": "experiment",
                "submitter_id": "BLGSP-71-06-00019",
                "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            }
        ),
    )
    assert resp.status_code == 200, resp.data
    resp_json = json.loads(resp.data)
    assert resp_json["entity_error_count"] == 0
    assert resp_json["created_entity_count"] == 1
    with pg_driver.session_scope():
        assert not pg_driver.nodes(md.Experiment).first()


def test_incorrect_project_error(client, pg_driver, cgci_blgsp, submitter):
    put_tcga_brca(client, submitter)
    resp = client.put(
        BLGSP_PATH,
        headers=submitter,
        data=json.dumps(
            {
                "type": "experiment",
                "submitter_id": "BLGSP-71-06-00019",
                "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            }
        ),
    )
    resp = client.put(
        BRCA_PATH,
        headers=submitter,
        data=json.dumps(
            {
                "type": "experiment",
                "submitter_id": "BLGSP-71-06-00019",
                "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            }
        ),
    )
    resp_json = json.loads(resp.data)
    assert resp.status_code == 400
    assert resp_json["code"] == 400
    assert resp_json["entity_error_count"] == 1
    assert resp_json["created_entity_count"] == 0
    assert (
        resp_json["entities"][0]["errors"][0]["type"] == "INVALID_PERMISSIONS"
    )  # noqa: E501


def test_insert_multiple_parents_and_export_by_ids(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    post_example_entities_together(client, submitter)
    path = BLGSP_PATH
    with open(os.path.join(DATA_DIR, "experimental_metadata.tsv"), "r") as f:
        headers = submitter
        headers["Content-Type"] = "text/tsv"
        resp = client.post(path, headers=headers, data=f.read())
        assert resp.status_code == 201, resp.data
    data = json.loads(resp.data)
    submitted_id = data["entities"][0]["id"]
    resp = client.get(
        "/v0/submission/CGCI/BLGSP/export/?ids={}".format(submitted_id),
        headers=headers,  # noqa: E501
    )
    str_data = str(resp.data)
    assert "BLGSP-71-experiment-01" in str_data
    assert "BLGSP-71-experiment-02" in str_data
    assert "experiments.submitter_id" in str_data


def test_timestamps(client, pg_driver, cgci_blgsp, submitter):
    test_post_example_entities(client, pg_driver, cgci_blgsp, submitter)
    with pg_driver.session_scope():
        case = pg_driver.nodes(md.Case).first()
        ct = case.created_datetime
        print(case.props)
        assert ct is not None, case.props


def test_disallow_cross_project_references(
    client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    put_tcga_brca(client, submitter)
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
        "cases": {"submitter_id": "BLGSP-71-06-00019"},
        "type": "diagnosis",
        "prior_malignancy": "no",
        "days_to_recurrence": -1,
        "days_to_last_known_disease_status": -1,
    }
    resp = client.put(BRCA_PATH, headers=submitter, data=json.dumps(data))
    assert resp.status_code == 400, resp.data


def test_delete_entity(client, pg_driver, cgci_blgsp, submitter):
    resp = client.put(
        BLGSP_PATH,
        headers=submitter,
        data=json.dumps(
            {
                "type": "experiment",
                "submitter_id": "BLGSP-71-06-00019",
                "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            }
        ),
    )
    assert resp.status_code == 200, resp.data
    did = resp.json["entities"][0]["id"]
    path = BLGSP_PATH + "entities/" + did
    resp = client.delete(path, headers=submitter)
    assert resp.status_code == 200, resp.data


def test_catch_internal_errors(
    monkeypatch, client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    """
    Monkey patch an essential function to just raise an error and assert that
    this error is caught and recorded as a transactional_error.
    """

    def just_raise_exception(self):
        raise Exception("test")

    monkeypatch.setattr(
        UploadTransaction, "pre_validate", just_raise_exception
    )  # noqa: E501
    try:
        r = put_example_entities_together(client, submitter)
        assert len(r.json["transactional_errors"]) == 1, r.data
    except:  # noqa: E722
        raise


def test_validator_error_types(client, pg_driver, cgci_blgsp, submitter):
    assert put_example_entities_together(client, submitter).status_code == 200

    r = client.put(
        BLGSP_PATH,
        headers=submitter,
        data=json.dumps(
            {
                "type": "sample",
                "cases": {"submitter_id": "BLGSP-71-06-00019"},
                "is_ffpe": "maybe",
                "sample_type": "Blood Derived Normal",
                "submitter_id": "BLGSP-71-06-00019",
                "longest_dimension": -1.0,
            }
        ),
    )
    errors = {e["keys"][0]: e["type"] for e in r.json["entities"][0]["errors"]}
    assert r.status_code == 400, r.data
    assert errors["is_ffpe"] == "INVALID_VALUE"
    assert errors["longest_dimension"] == "INVALID_VALUE"


def test_invalid_json(client, pg_driver, cgci_blgsp, submitter):
    resp = client.put(
        BLGSP_PATH,
        headers=submitter,
        data="""{
    "key1": "valid value",
    "key2": not a string,
}""",
    )
    print(resp.data)
    assert resp.status_code == 400
    assert "Expecting value" in resp.json["message"]


def test_get_entity_by_id(client, pg_driver, cgci_blgsp, submitter):
    post_example_entities_together(client, submitter)
    with pg_driver.session_scope():
        case_id = pg_driver.nodes(md.Case).first().node_id
    path = "/v0/submission/CGCI/BLGSP/entities/{case_id}".format(
        case_id=case_id
    )  # noqa: E501
    r = client.get(path, headers=submitter)
    assert r.status_code == 200, r.data
    assert r.json["entities"][0]["properties"]["id"] == case_id, r.data


def test_invalid_file_index(
    monkeypatch, client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    """
    Test that submitting an invalid data file doesn't create an index and an
    alias.
    """

    def fail_index_test(_):
        raise AssertionError("IndexClient tried to create index or alias")

    # Since the IndexClient should never be called to register anything if the
    # file is invalid, change the ``create`` and ``create_alias`` methods to
    # raise an error.
    monkeypatch.setattr(
        UploadTransaction,
        "index_client.create",
        fail_index_test,
        raising=False,  # noqa: E501
    )
    monkeypatch.setattr(
        UploadTransaction,
        "index_client.create_alias",
        fail_index_test,
        raising=False,  # noqa: E501
    )
    # Attempt to post the invalid entities.
    test_fnames = data_fnames + [
        "read_group.json",
        "submitted_unaligned_reads_invalid.json",
    ]
    resp = post_example_entities_together(
        client, submitter, data_fnames2=test_fnames
    )  # noqa: E501
    print(resp)


def test_valid_file_index(
    monkeypatch,
    client,
    pg_driver,
    cgci_blgsp,
    submitter,
    index_client,
    require_index_exists_off,
):
    """
    Test that submitting a valid data file creates an index and an alias.
    """

    # Update this dictionary in the patched functions to check that they are
    # called.

    # Attempt to post the valid entities.
    test_fnames = data_fnames + [
        "read_group.json",
        "submitted_unaligned_reads.json",
    ]  # noqa: E501
    resp = post_example_entities_together(
        client, submitter, data_fnames2=test_fnames
    )  # noqa: E501
    assert resp.status_code == 201, resp.data

    # this is a node that will have an indexd entry
    sur_entity = None
    for entity in resp.json["entities"]:
        if entity["type"] == "submitted_unaligned_reads":
            sur_entity = entity

    assert sur_entity, "No submitted_unaligned_reads entity created"
    assert index_client.get(sur_entity["id"]), "No indexd document created"


def test_submit_valid_tsv(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that we can submit a valid TSV file
    """

    data = {
        "type": "experiment",
        "submitter_id": "BLGSP-71-06-00019",
        "projects.id": "daa208a7-f57a-562c-a04a-7a7c77542c98",
    }

    # convert to TSV (save to file)
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data/experiment_tmp.tsv"
    )
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter="\t")
        dw.writeheader()
        dw.writerow(data)

    # read the TSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data

    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data


def test_submit_valid_csv(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that we can submit a valid CSV file
    """

    data = {
        "type": "experiment",
        "submitter_id": "BLGSP-71-06-00019",
        "projects.id": "daa208a7-f57a-562c-a04a-7a7c77542c98",
    }

    # convert to CSV (save to file)
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data/experiment_tmp.csv"
    )
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter=",")
        dw.writeheader()
        dw.writerow(data)

    # read the CSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data

    headers = submitter
    headers["Content-Type"] = "text/csv"
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data


def test_can_submit_with_asterisk_json(
    client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    """
    Test that we can submit when some fields have asterisks prepended
    """

    headers = submitter
    data = json.dumps(
        {
            "*type": "experiment",
            "*submitter_id": "BLGSP-71-06-00019",
            "*projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
        }
    )
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data


def test_can_submit_with_asterisk_tsv(
    client, pg_driver, cgci_blgsp, submitter
):  # noqa: E501
    """
    Test that we can submit when some fields have asterisks prepended
    """

    data = {
        "*type": "experiment",
        "*submitter_id": "BLGSP-71-06-00019",
        "*projects.id": "daa208a7-f57a-562c-a04a-7a7c77542c98",
    }
    # convert to TSV (save to file)
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data/experiment_tmp.tsv"
    )
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter="\t")
        dw.writeheader()
        dw.writerow(data)

    # read the TSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data

    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data


def test_export_entity_by_id(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    post_example_entities_together(client, submitter, extended_data_fnames)
    with pg_driver.session_scope():
        case_id = pg_driver.nodes(md.Case).first().node_id
    path = "/v0/submission/CGCI/BLGSP/export/?ids={case_id}".format(
        case_id=case_id
    )  # noqa: E501
    r = client.get(path, headers=submitter)
    assert r.status_code == 200, r.data
    assert r.headers["Content-Disposition"].endswith("tsv")

    path += "&format=json"
    r = client.get(path, headers=submitter)
    data = r.json
    assert data and len(data) == 1
    assert data[0]["id"] == case_id


def do_test_export(client, pg_driver, submitter, node_type, format_type):
    post_example_entities_together(client, submitter, extended_data_fnames)
    experimental_metadata_count = add_and_get_new_experimental_metadata_count(
        pg_driver
    )  # noqa: E501
    r = get_export_data(client, submitter, node_type, format_type, False)
    assert r.status_code == 200, r.data
    assert r.headers["Content-Disposition"].endswith(format_type)
    if format_type == "tsv":
        str_data = str(r.data, "utf-8")
        assert (
            len(str_data.strip().split("\n"))
            == experimental_metadata_count + 1  # noqa: E501
        )
        return str_data
    else:
        js_data = json.loads(r.data)
        assert len(js_data["data"]) == experimental_metadata_count
        return js_data


def get_export_data(client, submitter, node_type, format_type, without_id):
    path = "/v0/submission/CGCI/BLGSP/export/?node_label={}&format={}".format(
        node_type, format_type
    )
    if without_id:
        path += "&without_id=True"
    r = client.get(path, headers=submitter)
    return r


def test_export_all_node_types(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    do_test_export(
        client, pg_driver, submitter, "experimental_metadata", "tsv"
    )  # noqa: E501


def test_export_all_node_types_and_resubmit_json(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    js_id_data = do_test_export(
        client, pg_driver, submitter, "experimental_metadata", "json"
    )
    js_data = json.loads(
        get_export_data(
            client, submitter, "experimental_metadata", "json", True
        ).data  # noqa: E501
    )

    for o in js_id_data.get("data"):
        did = o["id"]
        path = BLGSP_PATH + "entities/" + did
        resp = client.delete(path, headers=submitter)
        assert resp.status_code == 200, resp.data

    headers = submitter
    resp = client.post(
        BLGSP_PATH, headers=headers, data=json.dumps(js_data["data"])
    )  # noqa: E501
    assert resp.status_code == 201, resp.data


def test_export_all_node_types_and_resubmit_tsv(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    str_id_data = do_test_export(
        client, pg_driver, submitter, "experimental_metadata", "tsv"
    )
    str_data = str(
        get_export_data(
            client, submitter, "experimental_metadata", "tsv", True
        ).data,  # noqa: E501
        "utf-8",
    )

    reader = csv.DictReader(StringIO(str_id_data), dialect="excel-tab")
    for row in reader:
        did = row["id"]
        path = BLGSP_PATH + "entities/" + did
        resp = client.delete(path, headers=submitter)
        assert resp.status_code == 200, resp.data

    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.post(BLGSP_PATH, headers=headers, data=str_data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 201, resp.data


def test_export_all_node_types_and_resubmit_json_with_empty_field(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    """
    Test we can export an entity with empty fields (as json) then resubmit it.
    The exported entity should have the empty fields omitted.
    """
    js_id_data = do_test_export(  # noqa: F841
        client, pg_driver, submitter, "experiment", "json"  # noqa: E501
    )
    assert js_id_data
    js_data = json.loads(
        get_export_data(client, submitter, "experiment", "json", True).data
    )
    nonempty = ["project_id", "submitter_id", "projects", "type"]
    print(js_data)
    for data in js_data["data"]:
        for key in data.keys():
            assert key in nonempty

    headers = submitter
    resp = client.put(
        BLGSP_PATH, headers=headers, data=json.dumps(js_data["data"])
    )  # noqa: E501
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data


def test_export_all_node_types_and_resubmit_tsv_with_empty_field(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    """
    Test we can export an entity with empty fields (as tsv) then resubmit it.
    The empty values of the exported entity should be empty strings.
    """
    str_id_data = do_test_export(  # noqa: F841
        client, pg_driver, submitter, "experiment", "tsv"  # noqa: E501
    )
    assert str_id_data
    str_data = get_export_data(
        client, submitter, "experiment", "tsv", True
    ).data  # noqa: E501

    nonempty = ["project_id", "submitter_id", "projects.code", "type"]
    tsv_output = csv.DictReader(
        StringIO(str_data.decode("utf-8")), delimiter="\t"
    )  # noqa: E501
    for row in tsv_output:
        for k, v in row.items():
            if k not in nonempty:
                assert v == ""

    str_data = str(str_data, "utf-8")
    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.put(BLGSP_PATH, headers=headers, data=str_data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data


def test_export_all_node_types_json(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    post_example_entities_together(client, submitter, extended_data_fnames)
    with pg_driver.session_scope() as s:
        case = pg_driver.nodes(md.Case).first()
        new_case = md.Case(str(uuid.uuid4()))
        new_case.props = case.props
        new_case.submitter_id = "case-2"
        s.add(new_case)
        case_count = pg_driver.nodes(md.Case).count()
    path = "/v0/submission/CGCI/BLGSP/export/?node_label=case&format=json"
    r = client.get(path, headers=submitter)
    assert r.status_code == 200, r.data
    assert r.headers["Content-Disposition"].endswith("json")
    js_data = json.loads(r.data)
    assert len(js_data["data"]) == case_count


def test_submit_export_encoding(client, pg_driver, cgci_blgsp, submitter):
    """Test that we can submit and export non-ascii characters without errors"""  # noqa: E501
    # submit metadata containing non-ascii characters
    headers = submitter
    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-submitter-ü",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
        }
    )
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data

    node_id = resp.json["entities"][0]["id"]

    # TSV single node export
    path = "/v0/submission/CGCI/BLGSP/export/?ids={}".format(node_id)
    r = client.get(path, headers=submitter)
    assert r.status_code == 200, r.data
    assert r.headers["Content-Disposition"].endswith("tsv")
    tsv_output = csv.DictReader(
        StringIO(r.data.decode("utf-8")), delimiter="\t"
    )  # noqa: E501
    row = next(tsv_output)
    assert row["submitter_id"] == "BLGSP-submitter-ü"

    # JSON single node export
    path += "&format=json"
    r = client.get(path, headers=submitter)
    assert len(r.json) == 1

    # TSV multiple node export
    path = "/v0/submission/CGCI/BLGSP/export/?node_label=experiment"
    r = client.get(path, headers=submitter)
    assert r.status_code == 200, r.data
    assert r.headers["Content-Disposition"].endswith("tsv")
    tsv_output = csv.DictReader(
        StringIO(r.data.decode("utf-8")), delimiter="\t"
    )  # noqa: E501
    row = next(tsv_output)
    assert row["submitter_id"] == "BLGSP-submitter-ü"

    # JSON multiple node export
    path += "&format=json"
    r = client.get(path, headers=submitter)
    assert len(r.json) == 1


def test_duplicate_submission(app, pg_driver, cgci_blgsp, submitter):
    """
    Make sure that concurrent transactions don't cause duplicate submission.
    """
    data = {
        "type": "experiment",
        "submitter_id": "BLGSP-71-06-00019",
        "projects.id": "daa208a7-f57a-562c-a04a-7a7c77542c98",
    }

    # convert to TSV (save to file)
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data/experiment_tmp.tsv"
    )
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter="\t")
        dw.writeheader()
        dw.writerow(data)

    # read the TSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data

    program, project = BLGSP_PATH.split("/")[3:5]
    tsv_data = TSVToJSONConverter().convert(data)[0]
    doc_args = [None, "tsv", data, tsv_data]
    utx1, utx2 = [
        UploadTransaction(
            program=program,
            project=project,
            role=ROLES["UPDATE"],
            logger=app.logger,
            flask_config=app.config,
            index_client=app.index_client,
            external_proxies=get_external_proxies(),
            db_driver=pg_driver,
        )
        for _ in range(2)
    ]

    response = ""
    with pg_driver.session_scope(can_inherit=False) as s1:
        with utx1:
            utx1.parse_doc(*doc_args)
            with pg_driver.session_scope(can_inherit=False) as s2:
                with utx2:
                    utx2.parse_doc(*doc_args)

                    with pg_driver.session_scope(session=s2):
                        utx2.flush()

                    with pg_driver.session_scope(session=s2):
                        utx2.post_validate()

                    with pg_driver.session_scope(session=s2):
                        utx2.commit()

            try:
                with pg_driver.session_scope(session=s1):
                    utx1.flush()
            except IntegrityError:
                s1.rollback()
                utx1.integrity_check()
                response = utx1.json
            # OperationalError in the case of SERIALIZABLE isolation_level
            except OperationalError:
                s1.rollback()
                utx1.integrity_check()
                response = utx1.json

    assert response["entity_error_count"] == 1
    assert response["code"] == 400
    assert (
        response["entities"][0]["errors"][0]["message"]
        == "experiment with {'project_id': 'CGCI-BLGSP', 'submitter_id': 'BLGSP-71-06-00019'} already exists in the DB"  # noqa: E501
    )

    with pg_driver.session_scope():
        assert pg_driver.nodes(md.Experiment).count() == 1


def test_zero_decimal_float(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that float values with a zero decimal are accepted by Sheepdog
    for properites of type "number" even if they look like integers.
    We are testing with TSV because the str values from TSV are cast
    to the proper type by Sheepdog.
    """
    resp = client.put(
        BLGSP_PATH,
        headers=submitter,
        data=json.dumps(
            [
                {
                    "type": "experiment",
                    "submitter_id": "BLGSP-71-06-00019",
                    "projects": {"code": "BLGSP"},
                },
                {
                    "type": "case",
                    "submitter_id": "BLGSP-71-case-01",
                    "experiments": {"submitter_id": "BLGSP-71-06-00019"},
                },
            ]
        ),
    )

    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data

    data = {
        "type": "sample",
        "submitter_id": "sample1",
        "cases.submitter_id": "BLGSP-71-case-01",
        "sample_volume": 2.0,
    }

    # convert to TSV (save to file)
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data/experiment_tmp.tsv"
    )
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter="\t")
        dw.writeheader()
        dw.writerow(data)

    # read the TSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data

    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data


def test_update_to_null_valid(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that updating a non required field to null works correctly
    """
    headers = submitter
    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            "experimental_description": "my desc",
            "number_samples_per_experimental_group": 1,
            "copy_numbers_identified": True,
        }
    )
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data
    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{json.loads(resp.data)['entities'][0]['id']}",  # noqa: E501
        headers=headers,
    )
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))

    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            "experimental_description": None,
            "number_samples_per_experimental_group": None,
            "copy_numbers_identified": None,
        }
    )
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data

    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{json.loads(resp.data)['entities'][0]['id']}",  # noqa: E501
        headers=headers,
    )
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert (
        json.loads(resp.data)["entities"][0]["properties"][
            "experimental_description"
        ]  # noqa: E501
        is None
    )
    assert (
        json.loads(resp.data)["entities"][0]["properties"][
            "number_samples_per_experimental_group"
        ]
        is None
    )
    assert (
        json.loads(resp.data)["entities"][0]["properties"]["indels_identified"]
        is None  # noqa: E501
    )


def test_update_to_null_invalid(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that updating a required field to null results in an error
    """
    headers = submitter
    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
        }
    )
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data
    entity_id = json.loads(resp.data)["entities"][0]["id"]

    data = json.dumps({"submitter_id": None})
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 400, resp.data

    data = json.dumps({"type": None})
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 400, resp.data

    data = json.dumps({"id": None})
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 400, resp.data

    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{entity_id}", headers=headers
    )  # noqa: E501
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert (
        json.loads(resp.data)["entities"][0]["properties"]["submitter_id"]
        == "BLGSP-71-06-00019"
    )
    assert (
        json.loads(resp.data)["entities"][0]["properties"]["type"]
        == "experiment"  # noqa: E501
    )
    assert json.loads(resp.data)["entities"][0]["properties"]["id"] == entity_id


def test_update_to_null_valid_tsv(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that we can update a TSV file with null
    """

    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            "experimental_description": "my desc",
            "number_samples_per_experimental_group": 1,
        }
    )

    headers = submitter
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data
    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{json.loads(resp.data)['entities'][0]['id']}",  # noqa: E501
        headers=headers,
    )
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))

    data = {
        "type": "experiment",
        "submitter_id": "BLGSP-71-06-00019",
        "projects.id": "daa208a7-f57a-562c-a04a-7a7c77542c98",
        "experimental_description": None,
        "number_samples_per_experimental_group": None,
    }

    # convert to TSV (save to file)
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data/experiment_tmp.tsv"
    )
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter="\t")
        dw.writeheader()
        dw.writerow(data)

    # read the TSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data
    print(data)

    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data

    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{json.loads(resp.data)['entities'][0]['id']}",  # noqa: E501
        headers=headers,
    )
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert (
        json.loads(resp.data)["entities"][0]["properties"][
            "experimental_description"
        ]  # noqa: E501
        is None
    )
    assert (
        json.loads(resp.data)["entities"][0]["properties"][
            "number_samples_per_experimental_group"
        ]
        is None
    )


def test_update_to_null_invalid_tsv(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that updating a required field (using TSV) to null results in an error
    """

    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            "type_of_sample": "sample type",
        }
    )

    headers = submitter
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data
    entity_id = json.loads(resp.data)["entities"][0]["id"]

    data = {
        "type": "experiment",
        "submitter_id": None,
        "projects.id": "daa208a7-f57a-562c-a04a-7a7c77542c98",
        "id": None,
    }

    # convert to TSV (save to file)
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data/experiment_tmp.tsv"
    )
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter="\t")
        dw.writeheader()
        dw.writerow(data)

    # read the TSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data

    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 400, resp.data

    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{entity_id}", headers=headers
    )  # noqa: E501
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert (
        json.loads(resp.data)["entities"][0]["properties"]["submitter_id"]
        == "BLGSP-71-06-00019"
    )
    assert json.loads(resp.data)["entities"][0]["properties"]["id"] == entity_id


def test_update_to_null_enum(client, pg_driver, cgci_blgsp, submitter):
    """
    Test that updating a non required enum field to null works correctly
    """
    headers = submitter
    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            "type_of_data": "Raw",
        }
    )
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data
    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{json.loads(resp.data)['entities'][0]['id']}",  # noqa: E501
        headers=headers,
    )
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))

    data = json.dumps(
        {
            "type": "experiment",
            "submitter_id": "BLGSP-71-06-00019",
            "projects": {"id": "daa208a7-f57a-562c-a04a-7a7c77542c98"},
            "type_of_data": None,
        }
    )
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert resp.status_code == 200, resp.data

    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{json.loads(resp.data)['entities'][0]['id']}",  # noqa: E501
        headers=headers,
    )
    print(json.dumps(json.loads(resp.data), indent=4, sort_keys=True))
    assert (
        json.loads(resp.data)["entities"][0]["properties"]["type_of_data"]
        is None  # noqa: E501
    )


def test_update_to_null_link(
    client, cgci_blgsp, submitter, require_index_exists_off
):  # noqa: E501
    """
    Test that updating a non required link to null works correctly
    """
    # create an entity with a link
    headers = submitter
    experiement_submitter_id = "BLGSP-71-06-00019"
    experimental_metadata = {
        "type": "experimental_metadata",
        "experiments": {"submitter_id": experiement_submitter_id},
        "data_type": "Experimental Metadata",
        "file_name": "CGCI-file-b.bam",
        "md5sum": "35b39360cc41a7b635980159aef265ba",
        "data_format": "some_format",
        "submitter_id": "BLGSP-71-experimental-01-b",  # noqa: F601
        "data_category": "data_file",
        "file_size": 42,
    }
    resp = client.put(
        BLGSP_PATH,
        headers=submitter,
        data=json.dumps(
            [
                {
                    "type": "experiment",
                    "submitter_id": experiement_submitter_id,
                    "projects": {"code": "BLGSP"},
                },
                experimental_metadata,
            ]
        ),
    )
    assert resp.status_code == 200, json.dumps(json.loads(resp.data), indent=2)

    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{json.loads(resp.data)['entities'][1]['id']}",  # noqa: E501
        headers=headers,
    )
    entity = json.loads(resp.data)["entities"][0]
    assert (
        entity["properties"]["experiments"][0]["submitter_id"]
        == experiement_submitter_id
    ), json.dumps(entity, indent=2)

    # update the entity by explicitly removing the link
    experimental_metadata["experiments"] = None
    resp = client.put(
        BLGSP_PATH, headers=headers, data=json.dumps(experimental_metadata)
    )
    assert resp.status_code == 400, json.dumps(json.loads(resp.data), indent=2)

    resp = client.get(
        f"/v0/submission/CGCI/BLGSP/entities/{json.loads(resp.data)['entities'][0]['id']}",  # noqa: E501
        headers=headers,
    )
    entity = json.loads(resp.data)
    assert "experiments" not in entity, json.dumps(entity, indent=2)


def test_submit_blank_link(
    client, pg_driver, cgci_blgsp, submitter, require_index_exists_off
):
    """
    Test that a TSV can be submitted with an empty link column,
    when the link is not required.
    """
    data = {
        "type": "experimental_metadata",
        "experiments.submitter_id": "",  # TSV link format
        "data_type": "Experimental Metadata",
        "file_name": "CGCI-file-b.bam",
        "md5sum": "35b39360cc41a7b635980159aef265ba",
        "data_format": "some_format",
        "submitter_id": "BLGSP-71-experimental-01-b",
        "data_category": "data_file",
        "file_size": 42,
    }

    # convert to TSV (save to file)
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "data/experimental_metadata_tmp.tsv",
    )
    with open(file_path, "w") as f:
        dw = csv.DictWriter(f, sorted(data.keys()), delimiter="\t")
        dw.writeheader()
        dw.writerow(data)

    # read the TSV data
    data = None
    with open(file_path, "r") as f:
        data = f.read()
    os.remove(file_path)  # clean up (delete file)
    assert data

    headers = submitter
    headers["Content-Type"] = "text/tsv"
    resp = client.put(BLGSP_PATH, headers=headers, data=data)
    assert resp.status_code == 200, resp.data
