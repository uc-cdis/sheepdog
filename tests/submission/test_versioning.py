import json
import os

import pytest

from tests.submission.test_endpoints import post_example_entities_together, DATA_DIR
from tests.submission.utils import data_fnames

def post_data_file_creation(client, headers):
    """Boilerplate setup code for some tests

    Submit nodes for creation and get back the data file associated
    with the submission.

    Args:
        client (fixture): fixture for making http requests
        headers (dict): http header with token
    Returns:
        string: UUID of data file from submission
    """

    test_fnames = (
        data_fnames
        + ['read_group.json', 'submitted_unaligned_reads.json']
    )

    with open(os.path.join(DATA_DIR, 'submitted_unaligned_reads.json'), 'r') as f:
        sur_json = json.loads(f.read())

    resp = post_example_entities_together(client,
                                          headers,
                                          data_fnames2=test_fnames)
    assert resp.status_code == 201, resp.data

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


def test_create_data_file_entity(client, index_client, pg_driver, cgci_blgsp, submitter):
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
    data_file = post_data_file_creation(client, submitter)

    indexd_doc = index_client.get(data_file['did'])

    # entry created in indexd
    assert indexd_doc

    # entry in indexd has metadata
    assert indexd_doc.did == data_file['did']
    assert indexd_doc.size == data_file['file_size']
    assert indexd_doc.file_name == data_file['file_name']
    assert indexd_doc.hashes['md5'] == data_file['md5sum']

    # entry in indexd has no version
    assert indexd_doc.version is None

def test_update_data_file_entity(client, index_client, pg_driver, cgci_blgsp, submitter):
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
    """
    pass

@pytest.mark.skipif('True', 'Not yet implemented')
def test_new_version_data_file_entity(client, index_client, pg_driver, cgci_blgsp, submitter):
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
