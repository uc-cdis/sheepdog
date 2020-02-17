"""
Tests for admin endpoint functionality.
"""
# pylint: disable=unused-argument, no-member

import json
from collections import defaultdict

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

    entity_types = defaultdict(int)
    for fname in data_fnames:
        fname = fname.split(".")[0]
        entity_types[fname] += 1
    resp = post_example_entities_together(client, headers, data_fnames2=test_fnames)
    assert resp.status_code == 201, resp.data

    submitted_entities = defaultdict(list)
    for entity in resp.json["entities"]:
        submitted_entities[entity["type"]].append(entity["id"])

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


def test_reassign(
    client, pg_driver, cgci_blgsp, index_client, submitter, require_index_exists_off
):
    """Try to reassign a node's remote URL

    Url:
        PUT: /admin/<program>/<project>/files/<file_uuid>/reassign
        data: {"s3_url": "s3://whatever/you/want"}
    """
    # Does not test authz.

    # Set up for http reassign action
    entities = post_blgsp_files(client, submitter)
    dids = entities["submitted_unaligned_reads"]
    s3_url = "s3://whatever/you/want"
    reassign_path = create_blgsp_url("/files/{}/reassign".format(dids[0]))
    data = json.dumps({"s3_url": s3_url})
    # http reassign action
    resp = client.put(reassign_path, headers=submitter, data=data)

    assert resp.status_code == 200, resp.data
    assert index_client.get(dids[0]), "Did not register with indexd?"
    assert s3_url in index_client.get(dids[0]).urls, "Did not successfully reassign"


def test_reassign_unauthorized(
    client,
    pg_driver,
    cgci_blgsp,
    submitter,
    index_client,
    require_index_exists_off,
    mock_arborist_requests,
):
    """Try to reassign a node's remote URL

    Url:
        PUT: /admin/<program>/<project>/files/<file_uuid>/reassign
        data: {"s3_url": "s3://whatever/you/want"}
    """
    # Just checks that this is guarded with an Arborist auth request.
    # (Does not check that the auth request is for the Sheepdog admin policy.)

    # Set up for http reassign action
    entities = post_blgsp_files(client, submitter)
    dids = entities["submitted_unaligned_reads"]
    s3_url = "s3://whatever/you/want"
    reassign_path = create_blgsp_url("/files/{}/reassign".format(dids[0]))
    data = json.dumps({"s3_url": s3_url})
    # Mock arborist auth requests so they return false
    mock_arborist_requests(authorized=False)
    # http reassign action
    resp = client.put(reassign_path, headers=submitter, data=data)

    assert resp.status_code == 403, resp.data
    assert index_client.get(dids[0]), "Index should have been created."
    assert len(index_client.get(dids[0]).urls) == 0, "No files have been uploaded"
    assert s3_url not in index_client.get(dids[0]).urls, "Should not have reassigned"
