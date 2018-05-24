"""
BACKWARDS COMPATIBILITY TESTING FOR GDC USE WITH SIGNPOST

Test upload entities (mostly data file handling and communication with
index service).

Essentially, there are only 2 cases being handled:
    1) id is provided, don't create file in index since assigning id's are
       not allowed
    2) no id is provided, create a new file in the index regardless of whether
       or not that file already exists in the index
"""
import flask
import copy

from test_upload import submit_first_experiment
from test_upload import submit_metadata_file

from utils import assert_positive_response
from utils import assert_negative_response
from utils import assert_single_entity_from_response

# Python 2 and 3 compatible
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch

BLGSP_PATH = '/v0/submission/CGCI/BLGSP/'

# some default values for data file submissions
DEFAULT_FILE_HASH = '00000000000000000000000000000001'
DEFAULT_FILE_SIZE = 1
DEFAULT_URL = 'test/url/test/0'
DEFAULT_SUBMITTER_ID = '0'
DEFAULT_UUID = 'bef870b0-1d2a-4873-b0db-14994b2f89bd'

DEFAULT_METADATA_FILE = {
    'type': 'experimental_metadata',
    'data_type': 'Experimental Metadata',
    'file_name': 'test-file',
    'md5sum': DEFAULT_FILE_HASH,
    'data_format': 'some_format',
    'submitter_id': DEFAULT_SUBMITTER_ID,
    'experiments': {
        'submitter_id': 'BLGSP-71-06-00019'
    },
    'data_category': 'data_file',
    'file_size': DEFAULT_FILE_SIZE,
    'state_comment': '',
    'urls': DEFAULT_URL
}


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_not_indexed(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp, monkeypatch):
    """
    Test node and data file creation when neither exist and no ID is provided.
    """
    monkeypatch.setitem(flask.current_app.config, 'USE_SIGNPOST', True)
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    get_index_uuid.return_value = None
    get_index_hash.return_value = None

    # signpost create will return a document
    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    create_index.return_value = document

    resp = submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # index creation should be called with no args for SIGNPOST
    assert create_index.call_count == 1
    args, kwargs = create_index.call_args_list[0]
    assert not args
    assert not kwargs

    # no support for aliases
    assert not create_alias.called

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_not_indexed_id_provided(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp, monkeypatch):
    """
    Test node and data file creation when neither exist and an ID is provided.

    NOTE: signpostclient will NOT create add a file to the index service if an ID
          is provided
    """
    monkeypatch.setitem(flask.current_app.config, 'USE_SIGNPOST', True)
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    get_index_uuid.return_value = None
    get_index_hash.return_value = None

    # signpost create will return a document
    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    create_index.return_value = document

    file = copy.deepcopy(DEFAULT_METADATA_FILE)
    file['id'] = DEFAULT_UUID
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=file)

    # no index creation
    assert not create_index.called

    # no support for aliases
    assert not create_alias.called

    # response
    assert_negative_response(resp)
    assert_single_entity_from_response(resp)


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_already_indexed(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp, monkeypatch):
    """
    Test submitting when the file is already indexed in the index client and
    no ID is provided. sheepdog should fall back on the hash/size of the file
    to find it in indexing service.

    NOTE: signpostclient will create another file in the index service regardless
          of whether or not the file exists. signpostclient does not
          have capabilities of searching for files based on hash/size
    """
    monkeypatch.setitem(flask.current_app.config, 'USE_SIGNPOST', True)
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    # signpostclient cannot find by hash/size
    get_index_hash.return_value = None
    get_index_uuid.return_value = None

    # signpost create will return a document
    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    create_index.return_value = document

    resp = submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # index creation should be called with no args for SIGNPOST
    assert create_index.call_count == 1
    args, kwargs = create_index.call_args_list[0]
    assert not args
    assert not kwargs

    # no support for aliases
    assert not create_alias.called

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_already_indexed_id_provided(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp, monkeypatch):
    """
    Test submitting when the file is already indexed in the index client and
    an id is provided in the submission.

    NOTE: signpostclient will NOT create add a file to the index service if an ID
          is provided
    """
    monkeypatch.setitem(flask.current_app.config, 'USE_SIGNPOST', True)
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    get_index_uuid.return_value = document

    # signpostclient cannot find by hash/size
    get_index_hash.return_value = None

    # signpost create will return a document
    create_index.return_value = document

    # only return the correct document by uuid IF the uuid provided is
    # the one from above
    def get_index_by_uuid(uuid):
        if uuid == document.did:
            return document
        else:
            return None
    get_index_uuid.side_effect = get_index_by_uuid

    file = copy.deepcopy(DEFAULT_METADATA_FILE)
    file['id'] = document.did
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=file)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # no support for aliases
    assert not create_alias.called

    # response
    assert_negative_response(resp)
    assert_single_entity_from_response(resp)
