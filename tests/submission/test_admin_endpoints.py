"""tests.submission.test_admin_endpoints

Tests for admin endpoint functionality.
"""
# pylint: disable=unused-argument, no-member

import json
import os.path

from gdcdatamodel import models as md
from tests.submission.test_endpoints import (
    BLGSP_PATH,
    post_example_entities_together,
)
from tests.submission.utils import data_fnames

def create_blgsp_url(path):
    base_url = '/v0/submission/admin/CGCI/BLGSP'
    if not path.startswith('/'):
        path = '/' + path
    return base_url + path

def post_blgsp_files(client, headers):
    """Boilerplate setup code for some tests

    Args:
        client (fixture): fixture for making http requests
        headers (dict): http header with token
    Returns:
        dict: map of entity type to UUID of submitted entities
    """

    test_fnames = (
        data_fnames
        + ['read_group.json', 'submitted_unaligned_reads.json']
    )

    entity_types = [fname.replace('.json', '') for fname in data_fnames]
    resp = post_example_entities_together(client,
                                          headers,
                                          data_fnames2=test_fnames)
    assert resp.status_code == 201, resp.data

    submitted_entities = {
        entity['type']: entity['id']
        for entity in resp.json['entities']
    }

    for entity_type in entity_types:
        assert entity_type in submitted_entities, 'entity not found in submission'

    return submitted_entities

def test_to_delete_with_admin(client, pg_driver, cgci_blgsp, submitter, admin):
    """Try to set the sysan of a node with admin credentials

    Url:
        DELETE: /admin/<program>/<project>/entities/<ids>/to_delete/<to_delete>
    """

    entities = post_blgsp_files(client, submitter)
    did = entities['submitted_unaligned_reads']

    base_delete_path = create_blgsp_url('/entities/{}/to_delete/'.format(did))
    to_delete_true = base_delete_path + 'true'
    to_delete_false = base_delete_path + 'false'

    resp = client.delete(to_delete_true, headers=admin)
    assert resp.status_code == 200, resp.data
    with pg_driver.session_scope():
        sur_node = pg_driver.nodes(md.SubmittedUnalignedReads).first()
        assert sur_node
        assert sur_node.sysan.get('to_delete') is True

    resp = client.delete(to_delete_false, headers=admin)
    assert resp.status_code == 200, resp.data
    with pg_driver.session_scope():
        sur_node = pg_driver.nodes(md.SubmittedUnalignedReads).first()
        assert sur_node
        assert sur_node.sysan.get('to_delete') is False

def test_to_delete_without_admin(client, pg_driver, cgci_blgsp, submitter):
    """Try to set the sysan of a node without having admin credentials

    Url:
        DELETE: /admin/<program>/<project>/entities/<ids>/to_delete/<to_delete>
    """

    entities = post_blgsp_files(client, submitter)
    did = entities['submitted_unaligned_reads']

    base_delete_path = create_blgsp_url('/entities/{}/to_delete/'.format(did))
    to_delete_true = base_delete_path + 'true'
    to_delete_false = base_delete_path + 'false'

    resp = client.delete(to_delete_true, headers=submitter)
    # has no access to the endpoint
    assert resp.status_code == 403, resp.data
    # is not deleted and has no to_delete sysan set
    with pg_driver.session_scope():
        sur_node = pg_driver.nodes(md.SubmittedUnalignedReads).first()
        assert sur_node
        assert sur_node.sysan.get('to_delete') is None

    resp = client.delete(to_delete_false, headers=submitter)
    # has no access to the endpoint
    assert resp.status_code == 403, resp.data
    # is not deleted and has no sysan set
    with pg_driver.session_scope():
        sur_node = pg_driver.nodes(md.SubmittedUnalignedReads).first()
        assert sur_node
        assert sur_node.sysan.get('to_delete') is None

def test_reassign_with_admin(client, pg_driver, cgci_blgsp, submitter, index_client, admin):
    """Try to reassign a node's remote URL

    Url:
        PUT: /admin/<program>/<project>/files/<file_uuid>/reassign
        data: {"s3_url": "s3://whatever/you/want"}
    """

    entities = post_blgsp_files(client, submitter)
    did = entities['submitted_unaligned_reads']
    s3_url = 's3://whatever/you/want'

    reassign_path = create_blgsp_url('/files/{}/reassign'.format(did))
    data = json.dumps({'s3_url': s3_url})

    resp = client.put(reassign_path, headers=admin, data=data)
    assert resp.status_code == 200, resp.data
    assert index_client.get(did), 'Did not register with indexd?'
    assert s3_url in index_client.get(did).urls, 'Did not successfully reassign'

def test_reassign_without_admin(client, pg_driver, cgci_blgsp, submitter, index_client):
    """Try to reassign a node's remote URL

    Url:
        PUT: /admin/<program>/<project>/files/<file_uuid>/reassign
        data: {"s3_url": "s3://whatever/you/want"}
    """

    entities = post_blgsp_files(client, submitter)
    did = entities['submitted_unaligned_reads']
    s3_url = 's3://whatever/you/want'

    reassign_path = create_blgsp_url('/files/{}/reassign'.format(did))
    data = json.dumps({'s3_url': s3_url})

    resp = client.put(reassign_path, headers=submitter, data=data)
    assert resp.status_code == 403, resp.data
    assert index_client.get(did), 'Index should have been created.'
    assert len(index_client.get(did).urls) == 0, 'No files have been uploaded'
    assert s3_url not in index_client.get(did).urls, 'Should not have reassigned'
