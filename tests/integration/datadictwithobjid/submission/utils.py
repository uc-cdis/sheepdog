import os
import re
import uuid
import indexclient

from datamodelutils import models


DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# https://stackoverflow.com/questions/373194/python-regex-for-md5-hash
re_md5 = re.compile(r"(i?)(?<![a-z0-9])[a-f0-9]{32}(?![a-z0-9])")

data_fnames = [
    "experiment.1.json",
    "experiment.2.json",
    "case.json",
    "sample.json",
    "aliquot.json",
    "demographic.json",
    "diagnosis.json",
    "exposure.json",
    "treatment.json",
]
extended_data_fnames = data_fnames + ["experimental_metadata.json"]

PATH = "/v0/submission/graphql"
BLGSP_PATH = "/v0/submission/CGCI/BLGSP/"
BRCA_PATH = "/v0/submission/TCGA/BRCA/"


def put_entity_from_file(
    client, file_path, submitter, put_path=BLGSP_PATH, validate=True
):
    with open(os.path.join(DATA_DIR, file_path), "r") as f:
        entity = f.read()
    r = client.put(put_path, headers=submitter(put_path, "put"), data=entity)
    if validate:
        assert r.status_code == 200, r.data
    return r


def reset_transactions(pg_driver):
    with pg_driver.session_scope() as s:
        s.query(models.submission.TransactionSnapshot).delete()
        s.query(models.submission.TransactionDocument).delete()
        s.query(models.submission.TransactionLog).delete()


def patch_indexclient(monkeypatch):

    called = {"create": False, "create_alias": False}

    def check_hashes(hashes):
        assert hashes is not None
        assert "md5" in hashes
        assert re_md5.match(hashes["md5"]) is not None

    def check_uuid4(
        self, did=None, urls=None, hashes=None, size=None, metadata=None, file_name=None
    ):
        """
        Using code from: https://gist.github.com/ShawnMilo/7777304
        """
        called["create"] = True
        # Check for valid UUID.
        try:
            val = uuid.UUID(did, version=4)
            assert val.hex == did.replace("-", "")
        except Exception:
            raise AssertionError("invalid uuid")
        check_hashes(hashes)

    def check_alias(
        self,
        record,
        size=None,
        hashes=None,
        release=None,
        metastring=None,
        host_authorities=None,
        keeper_authority=None,
    ):
        called["create_alias"] = True
        check_hashes(hashes)
        assert isinstance(record, str)

    monkeypatch.setattr(indexclient.client.IndexClient, "create", check_uuid4)
    monkeypatch.setattr(indexclient.client.IndexClient, "create_alias", check_alias)
    return called


def assert_positive_response(resp):
    assert resp.status_code == 200, resp.data
    entities = resp.json["entities"]
    for entity in entities:
        assert not entity["errors"]
    assert resp.json["success"] is True


def assert_negative_response(resp, on_entity=True):
    assert resp.status_code != 200, resp.data
    entities = resp.json["entities"]

    # check if at least one entity has an error
    if on_entity:
        entity_errors = [entity["errors"] for entity in entities if entity["errors"]]
        assert entity_errors

    assert resp.json["success"] is False


def assert_single_entity_from_response(resp):
    entities = resp.json["entities"]
    assert len(entities) == 1
    return entities[0]
