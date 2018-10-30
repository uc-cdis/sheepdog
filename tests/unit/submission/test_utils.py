import pytest
from gdcdatamodel import models
from indexclient.client import Document

from sheepdog import utils
from sheepdog.errors import UserError


def generate_check_action_data(action='delete'):
    """Generate generic data to use with check_action_allowed_for_file"""

    uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
    s3_url = 's3://url/bucket/key'
    file_state = 'validated'

    indexd_doc = {
        'version': None,
        'metadata': {},
        'urls_metadata': {
            s3_url: {
                'state': file_state,
                'type': 'primary',
            }
        }
    }

    indexd_doc = Document(None, uuid, indexd_doc)
    node = models.Experiment(uuid)
    node.batch_id = None

    return indexd_doc, node, action, file_state, s3_url

def test_valid_check_action_allowed_for_file():
    """Test if a file can be deleted

    If it passes the check then no exception is raised and nothing is returned.
    """
    indexd_doc, node, action, file_state, s3_url = generate_check_action_data()

    result = utils.check_action_allowed_for_file(
        indexd_doc,
        node,
        action,
        file_state,
        s3_url,
    )

    assert result is None


@pytest.mark.parametrize('release_number, version', [
    (None, 1),
    (1, None),
    (1, 1),
])
def test_released_check_action_allowed_for_file(release_number, version):
    """If either a version number or release number is set it's released

    Released nodes cannot be deleted. The check will raise an exception
    if a user tries to deleted a released node.
    """

    indexd_doc, node, action, file_state, s3_url = generate_check_action_data()
    indexd_doc.metadata['release_number'] = release_number
    indexd_doc.version = version

    message = 'Cannot delete a released node. Should have thrown a UserError.'
    with pytest.raises(UserError, message=message):
        utils.check_action_allowed_for_file(
            indexd_doc,
            node,
            action,
            file_state,
            s3_url,
        )

def test_submitted_check_action_allowed_for_file():
    """If a node has a batch_id then it is submitted

    Submitted nodes cannot be deleted. The check will raise an exception
    if a user tries to deleted a submitted node.
    """

    indexd_doc, node, action, file_state, s3_url = generate_check_action_data()
    node.batch_id = 1

    message = 'Cannot delete a submitted node. Should have thrown a UserError'
    with pytest.raises(UserError, message=message):
        utils.check_action_allowed_for_file(
            indexd_doc,
            node,
            action,
            file_state,
            s3_url,
        )
