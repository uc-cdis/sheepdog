import json
import os

import pytest
from gdcdatamodel import models as md

from tests.submission.test_endpoints import (
    post_example_entities_together,
    put_example_entities_together,
    DATA_DIR,
)
from tests.submission.utils import data_fnames

def data_file_creation(client, headers, method='post'):
    """Boilerplate setup code for some tests

    Submit nodes for creation and get back the data file associated
    with the submission.

    Args:
        client (fixture): fixture for making http requests
        headers (dict): http header with token
    Returns:
        string: UUID of data file from submission
    """

    test_fnames = data_fnames + ['read_group.json']

    if method == 'post':
        sur_filename = 'submitted_unaligned_reads.json'
        resp = post_example_entities_together(
            client,
            headers,
            data_fnames2=test_fnames + [sur_filename])
    elif method == 'put':
        sur_filename = 'submitted_unaligned_reads_new.json'
        resp = put_example_entities_together(
            client,
            headers,
            data_fnames2=test_fnames + [sur_filename])

    assert resp.status_code in (200, 201), resp.data

    with open(os.path.join(DATA_DIR, sur_filename), 'r') as f:
        sur_json = json.loads(f.read())

    sur_uuid = [
        entity['id']
        for entity in resp.json['entities']
        if entity['type'] == 'submitted_unaligned_reads'
    ][0]

    return {
        'did': sur_uuid,
        'file_size': sur_json['file_size'],
        'file_name': sur_json['file_name'],
        'md5sum': sur_json['md5sum'],
    }

def test_create_data_file_entity(client, indexd_server, indexd_client, pg_driver, cgci_blgsp, submitter):
    """Create a new node in the database.

    Success conditions:
        - Node created in database
        - Entry created in indexd
        - Entry in indexd has pertinent metadata
        - Entry in indexd has no version
            - Exactly one entry in indexd can have no version number
            - Nodes that have not been released do not have a version
            associated with them
    """

    # node created in database
    data_file = data_file_creation(client, submitter, method='post')

    # entry created in indexd
    indexd_doc = indexd_client.get(data_file['did'])
    assert indexd_doc

    # entry in indexd has metadata
    assert indexd_doc.did == data_file['did']
    assert indexd_doc.size == data_file['file_size']
    assert indexd_doc.file_name == data_file['file_name']
    assert indexd_doc.hashes['md5'] == data_file['md5sum']

    # entry in indexd has no version
    assert indexd_doc.version is None

def test_update_data_file_entity(app, client, indexd_server, indexd_client, pg_driver, cgci_blgsp, submitter):
    """Update an existing node in the database.

    The API allows a user to update a node with new information. This new
    information can be a partial update or a full node replace. On an update
    the indexd document will get deleted and recreated with the same old
    fields and the new fields supplied by the user.

    Success conditions:
        - Node already in database
        - Entry already in indexd
        - Update to node in indexd does not have a version associated
        - Indexd entry has new information supplied by the user
        - Exactly one entry in indexd can have no version number
        - Base IDs between the two same nodes should be the same
    """

    # simulate a project api that lets you do this
    app.config['CREATE_REPLACEABLE'] = True

    # node already in database
    data_file = data_file_creation(client, submitter, method='post')

    # entry already in indexd
    original_doc = indexd_client.get(data_file['did'])

    # update node, causing new information in indexd
    new_data_file = data_file_creation(client, submitter, method='put')
    new_doc = indexd_client.get(new_data_file['did'])

    with pg_driver.session_scope():
        sur_node = pg_driver.nodes(md.SubmittedUnalignedReads).first()
        # check if metadata about the file changed
        assert sur_node.node_id == new_data_file['did']
        assert sur_node.file_name == new_data_file['file_name']
        assert sur_node.file_size == new_data_file['file_size']
        assert sur_node.md5sum == new_data_file['md5sum']

    # updated entry in indexd has correct metadata
    assert new_doc
    assert new_doc.did == new_data_file['did']
    assert new_doc.size == new_data_file['file_size']
    assert new_doc.file_name == new_data_file['file_name']
    assert new_doc.hashes['md5'] == new_data_file['md5sum']

    # new fields
    assert new_doc.size != original_doc.size
    assert new_doc.file_name != original_doc.file_name
    assert new_doc.hashes['md5'] != original_doc.hashes['md5']

    # same did/baseid
    assert new_doc.did == original_doc.did
    assert new_doc.baseid == original_doc.baseid

    # entry in indexd has no version
    assert new_doc.version is None

@pytest.mark.skipif('True', 'Not yet implemented')
def test_new_version_data_file_entity(client, indexd_server, indexd_client, pg_driver, cgci_blgsp, submitter):
    """Update an existing node in the database.

    After a node is considered release, it receives a new version number
    in indexd.

    Success conditions:
        - Node in the database
        - Entry in indexd
        - Entry in indexd has at least one node marked with a version
        - Entry in indexd has exactly one node that has no version associated
    """
    pass
