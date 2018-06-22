import pytest
from gdcdatamodel import models as md

from tests.integration.submission.utils import (
    put_example_entities_together,
    data_file_creation,
    data_fnames,
    release_indexd_doc,
)


def test_create_data_file_entity(
        client, indexd_server, indexd_client, pg_driver, cgci_blgsp, submitter):
    """
    Create a new node in the database.

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
    data_file = data_file_creation(
        client,
        submitter,
        method='post',
        sur_filename='submitted_unaligned_reads.json',
    )

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


# simulate a project's api that lets you do this
@pytest.mark.config_toggle(
    parameters={
        'CREATE_REPLACEABLE': True,
        'ENFORCE_FILE_HASH_SIZE_UNIQUENESS': False,
        'IGNORE_PROJECT_STATE': True,
        'IS_GDC': True,
    }
)
def test_update_data_file_entity(
        client_toggled, indexd_server, indexd_client, pg_driver, cgci_blgsp, submitter):
    """
    Update an existing node in the database.

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
        - Cannot update a node if it is in state submitted
    """

    # node already in database
    data_file = data_file_creation(
        client_toggled,
        submitter,
        method='post',
        sur_filename='submitted_unaligned_reads.json',
    )

    # entry already in indexd
    original_doc = indexd_client.get(data_file['did'])

    # make file 'uploaded'
    original_doc.urls_metadata[original_doc.urls[0]]['state'] = 'uploaded'
    original_doc.patch()

    # update node, causing new information in indexd
    new_data_file = data_file_creation(
        client_toggled,
        submitter,
        method='put',
        sur_filename='submitted_unaligned_reads_new.json',
    )
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

    # make sure that a node cannot be updated after it's in state submitted

    # manually change file state to submitted
    with pg_driver.session_scope():
        sur = pg_driver.nodes(md.SubmittedUnalignedReads).one()
        sur.state = 'submitted'

    # attempt to update the node in state submitted
    resp = put_example_entities_together(
        client_toggled,
        submitter,
        data_fnames2=data_fnames +
        ['read_group.json', 'submitted_unaligned_reads_new.json']
    )

    assert resp.status_code == 400, 'indexd doc state should not be in "submitted" state'


# simulate a project's api that lets you do this
@pytest.mark.config_toggle(parameters={'CREATE_REPLACEABLE': True})
def test_creating_new_versioned_file(
        client_toggled, indexd_server, indexd_client, pg_driver, cgci_blgsp, submitter):
    """
    Create a new version of a file
    """

    def create_node(version_number=None):
        """Create a node and possibly assign it a version

        Args:
            version_number (int): whole number to indicate a version

        Returns:
            str: UUID of node submitted to the api
        """

        resp = data_file_creation(
            client_toggled,
            submitter,
            method='put',
            sur_filename='submitted_unaligned_reads.json',
        )

        did = resp['did']
        with pg_driver.session_scope():
            query = pg_driver.nodes(md.SubmittedUnalignedReads)

            # only one in the database
            assert query.count() == 1

            sur_node = query.first()
            assert sur_node
            assert len(sur_node.read_groups) == 1
            assert did == sur_node.node_id

        indexd_doc = indexd_client.get(did)
        assert indexd_doc, 'No doc created for {}'.format(did)
        assert indexd_doc.version is None, 'Should not have a version yet'

        # only give a version number if one is supplied
        if version_number:
            # simulate release by incrementing all versions for similar docs
            release_indexd_doc(pg_driver, indexd_client, did)

            indexd_doc = indexd_client.get(did)
            message = 'Should have been assigned version {}'.format(version_number)
            assert indexd_doc.version == version_number, message

        return resp['did']

    # create nodes and release a few times, just to be sure
    # create versions 1, 2, 3, None (no version on the last one)
    accepted_versions = ['1', '2', '3', None]
    uuids = [create_node(version_number=v) for v in accepted_versions]
    created_versions = [indexd_client.get(uuid).version for uuid in uuids]
    # this order is guaranteed
    for created, accepted in zip(created_versions, accepted_versions):
        assert created == accepted
