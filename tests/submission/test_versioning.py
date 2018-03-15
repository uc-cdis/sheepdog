import pytest

def test_create_data_file_entity(client, pg_driver, cgci_blgsp, submitter):
    """Create a new node in the database.

    Success conditions:
        - Node created in database
        - Entry created in indexd
        - Node in indexd has pertinent metadata
        - Node in indexd has no version
            - Exactly one entry in indexd without a version
            - Nodes that have not been released do not have a version
            associated with them
    """
    pass

def test_update_data_file_entity(client, pg_driver, cgci_blgsp, submitter):
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
        - Exactly one entry in indexd without version number
    """
    pass

@pytest.mark.skipif('True', 'Not yet implemented')
def test_new_version_data_file_entity(client, pg_driver, cgci_blgsp, submitter):
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
