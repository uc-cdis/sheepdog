"""
Tests for admin endpoint functionality.
"""
# pylint: disable=unused-argument, no-member

import json

import pytest

from datamodelutils import models as md
from tests.integration.datadict.submission.test_endpoints import (
    post_example_entities_together,
)
from tests.integration.datadict.submission.utils import data_fnames


def create_blgsp_url(path):
    base_url = "/v0/submission/admin/CGCI/BLGSP"
    if not path.startswith("/"):
        path = "/" + path
    return base_url + path


def post_blgsp_files(client, headers):
    """Boilerplate setup code for some tests

    Args:
        client (fixture): fixture for making http requests
        headers (dict): http header with token
    Returns:
        dict: map of entity type to UUID of submitted entities
    """

    test_fnames = data_fnames + ["read_group.json", "submitted_unaligned_reads.json"]

    # entity_types = [fname.replace(".json", "") for fname in data_fnames]
    entity_types = {}
    for fname in data_fnames:
        fname = fname.split(".")[0]
        if fname not in entity_types:
            entity_types[fname] = 1
        else:
            entity_types[fname] = entity_types.get(fname) + 1
    resp = post_example_entities_together(client, headers, data_fnames2=test_fnames)
    assert resp.status_code == 201, resp.data

    submitted_entities = {}
    for entity in resp.json["entities"]:
        if entity["type"] not in submitted_entities:
            submitted_entities[entity["type"]] = [entity["id"]]
        else:
            submitted_entities.get(entity["type"]).append(entity["id"])

    for k, v in entity_types.items():
        assert k in submitted_entities, "entity not found in submission"
        assert v == len(submitted_entities.get(k))

    return submitted_entities


@pytest.mark.parametrize(
    "headers,status_code,to_delete",
    [("submitter", 403, None), ("admin", 200, True), ("admin", 200, False)],
)
def test_to_delete(
    headers,
    status_code,
    to_delete,
    request,
    client,
    pg_driver,
    cgci_blgsp,
    submitter,
    require_index_exists_off,
    mock_arborist_requests,
):
    """Try to set the sysan of a node with and without delete access

    Url:
        DELETE: /admin/<program>/<project>/entities/<ids>/to_delete/<to_delete>
    """

    headers = request.getfixturevalue(headers)

    # submit files as submitter
    entities = post_blgsp_files(client, submitter)
    dids = entities["submitted_unaligned_reads"]

    base_delete_path = create_blgsp_url("/entities/{}/to_delete/".format(dids[0]))
    to_delete_path = base_delete_path + str(to_delete).lower()

    if status_code != 200:
        mock_arborist_requests(authorized=False)

    resp = client.delete(to_delete_path, headers=headers)
    assert resp.status_code == status_code, resp.data
    with pg_driver.session_scope():
        sur_node = pg_driver.nodes(md.SubmittedUnalignedReads).first()
        assert sur_node
        assert sur_node.sysan.get("to_delete") is to_delete


def do_reassign(client, headers):
    """Perform the http reassign action

    Args:
        client (pytest.Fixture): Allows you to mock http requests through flask
        headers (dict): http headers with token

    Returns:
        requests.Response: http response from doing reassign request
        string: did
        string: s3 url that you changed it to
    """

    entities = post_blgsp_files(client, headers)
    dids = entities["submitted_unaligned_reads"]
    s3_url = "s3://whatever/you/want"

    reassign_path = create_blgsp_url("/files/{}/reassign".format(dids[0]))
    data = json.dumps({"s3_url": s3_url})

    return client.put(reassign_path, headers=headers, data=data), dids[0], s3_url


def test_reassign_with_admin(
    client,
    pg_driver,
    cgci_blgsp,
    submitter,
    index_client,
    admin,
    require_index_exists_off,
):
    """Try to reassign a node's remote URL

    Url:
        PUT: /admin/<program>/<project>/files/<file_uuid>/reassign
        data: {"s3_url": "s3://whatever/you/want"}
    """

    resp, did, s3_url = do_reassign(client, admin)
    assert resp.status_code == 200, resp.data
    assert index_client.get(did), "Did not register with indexd?"
    assert s3_url in index_client.get(did).urls, "Did not successfully reassign"


def test_reassign_without_admin(
    client, pg_driver, cgci_blgsp, submitter, index_client, require_index_exists_off
):
    """Try to reassign a node's remote URL

    Url:
        PUT: /admin/<program>/<project>/files/<file_uuid>/reassign
        data: {"s3_url": "s3://whatever/you/want"}
    """

    resp, did, s3_url = do_reassign(client, submitter)
    assert resp.status_code == 403, resp.data
    assert index_client.get(did), "Index should have been created."
    assert len(index_client.get(did).urls) == 0, "No files have been uploaded"
    assert s3_url not in index_client.get(did).urls, "Should not have reassigned"
