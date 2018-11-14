"""
Test upload entities (mostly data file handling and communication with
index service).
"""
import json
import copy
import os

from test_endpoints import put_cgci_blgsp

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


def submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp):
    put_cgci_blgsp(client, admin)

    # first submit experiment
    data = json.dumps({
        'type': 'experiment',
        'submitter_id': 'BLGSP-71-06-00019',
        'projects': {
            'id': 'daa208a7-f57a-562c-a04a-7a7c77542c98'
        }
    })
    resp = client.put(BLGSP_PATH, headers=submitter, data=data)
    assert resp.status_code == 200, resp.data


def submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=None):
    data = data or DEFAULT_METADATA_FILE
    put_cgci_blgsp(client, admin)
    data = json.dumps(data)
    resp = client.put(BLGSP_PATH, headers=submitter, data=data)
    return resp


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_not_indexed(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp,
        require_index_exists_off):
    """
    Test node and data file creation when neither exist and no ID is provided.
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    get_index_uuid.return_value = None
    get_index_hash.return_value = None

    resp = submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # index creation
    assert create_index.call_count == 1
    _, kwargs = create_index.call_args_list[0]
    assert 'did' in kwargs
    did = kwargs['did']
    assert 'hashes' in kwargs
    assert kwargs['hashes'].get('md5') == DEFAULT_FILE_HASH
    assert 'urls' in kwargs
    assert DEFAULT_URL in kwargs['urls']

    # alias creation
    assert create_alias.called

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == did


def test_tsv_submission_handle_array_type(client):
    """
    When submitting a TSV file, array fields should be converted to lists.
    """

    file_data = copy.deepcopy(DEFAULT_METADATA_FILE)
    file_data['array_field'] = ' code a, codeb '

    # convert the file to TSV
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data/experimental_metadata.tsv')
    with open(file_path, 'w') as f:
        import csv
        dw = csv.DictWriter(f, sorted(file_data.keys()), delimiter='\t')
        dw.writeheader()
        dw.writerow(file_data)

    # read the TSV data
    doc = None
    with open(file_path, 'r') as f:
        doc = f.read()
    assert doc

    from sheepdog.utils.transforms import TSVToJSONConverter
    data, errors = TSVToJSONConverter().convert(doc)
    assert data
    assert len(data) == 1
    assert not errors

    # make sure the array is handled properly
    array = data[0]['array_field']
    assert isinstance(array, list)
    assert array == ['code a', 'codeb']


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_not_indexed_id_provided(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp,
        require_index_exists_off):
    """
    Test node and data file creation when neither exist and an ID is provided.
    That ID should be used for the node and file index creation
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    get_index_uuid.return_value = None
    get_index_hash.return_value = None

    file = copy.deepcopy(DEFAULT_METADATA_FILE)
    file['id'] = DEFAULT_UUID
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=file)

    # index creation
    assert create_index.call_count == 1
    args, kwargs = create_index.call_args_list[0]
    assert 'did' in kwargs
    did = kwargs['did']
    assert 'hashes' in kwargs
    assert kwargs['hashes'].get('md5') == DEFAULT_FILE_HASH
    assert 'urls' in kwargs
    assert DEFAULT_URL in kwargs['urls']

    # alias creation
    assert create_alias.called

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert did == DEFAULT_UUID
    assert entity['id'] == DEFAULT_UUID


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_already_indexed(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting when the file is already indexed in the index client and
    no ID is provided. sheepdog should fall back on the hash/size of the file
    to find it in indexing service.
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    get_index_hash.return_value = document

    # only return the correct document by uuid IF the uuid provided is
    # the one from above
    def get_index_by_uuid(uuid):
        if uuid == document.did:
            return document
        else:
            return None
    get_index_uuid.side_effect = get_index_by_uuid

    resp = submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == document.did


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_already_indexed_id_provided(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting when the file is already indexed in the index client and
    an id is provided in the submission.
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    get_index_uuid.return_value = document
    get_index_hash.return_value = document

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

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == document.did


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_update_url(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting the same data again but updating the URL field (should
    get added to the indexed file in index service).
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    document.urls = [DEFAULT_URL]
    get_index_hash.return_value = document

    # only return the correct document by uuid IF the uuid provided is
    # the one from above
    def get_index_by_uuid(uuid):
        if uuid == document.did:
            return document
        else:
            return None
    get_index_uuid.side_effect = get_index_by_uuid

    submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # now submit again but change url
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=updated_file)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # make sure new url are in the document and patch gets called
    assert document.urls == [new_url]
    assert document.patch.called

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'update'

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == document.did


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_update_multiple_urls(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting the same data again but updating the URL field (should
    get added to the indexed file in index service).
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    document.urls = [DEFAULT_URL]
    get_index_hash.return_value = document

    # only return the correct document by uuid IF the uuid provided is
    # the one from above
    def get_index_by_uuid(uuid):
        if uuid == document.did:
            return document
        else:
            return None
    get_index_uuid.side_effect = get_index_by_uuid

    submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # now submit again but change url
    new_url = 'some/new/url/location/to/add'
    another_new_url = 'some/other/url'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)

    # comma separated list of urls INCLUDING the url that's already there
    updated_file['urls'] = DEFAULT_URL + ',' + new_url + ',' + another_new_url
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=updated_file)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # make sure original url and new url are in the document and patch gets called
    assert DEFAULT_URL in document.urls
    assert new_url in document.urls
    assert another_new_url in document.urls
    assert document.patch.called

    # make sure no duplicates were added
    assert len(document.urls) == 3

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'update'

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == document.did


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_update_url_id_provided(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting the same data again (with the id provided) and updating the
    URL field (should get added to the indexed file in index service).
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    document.urls = [DEFAULT_URL]
    get_index_hash.return_value = document

    # only return the correct document by uuid IF the uuid provided is
    # the one from above
    def get_index_by_uuid(uuid):
        if uuid == document.did:
            return document
        else:
            return None
    get_index_uuid.side_effect = get_index_by_uuid

    submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # now submit again but change url
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['id'] = document.did
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=updated_file)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # make sure new url are in the document and patch gets called
    assert document.urls == [new_url]
    assert document.patch.called

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'update'

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == document.did


""" ----- TESTS THAT SHOULD RESULT IN SUBMISSION FAILURES ARE BELOW  ----- """


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_update_url_invalid_id(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting the same data again (with the WRONG id provided).
    i.e. ID provided doesn't match the id from the index service for the file
         found with the hash/size provided

    FIXME: the 1:1 between node id and index/file id is temporary so this
           test may need to be modified in the future
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    document.urls = [DEFAULT_URL]
    get_index_hash.return_value = document

    # the uuid provided doesn't have a matching indexed file
    get_index_uuid.return_value = None

    submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # now submit again but change url
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['id'] = DEFAULT_UUID
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=updated_file)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # make sure original url is still there and new url is NOT
    assert DEFAULT_URL in document.urls
    assert new_url not in document.urls

    # response
    assert_negative_response(resp)
    assert_single_entity_from_response(resp)

@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_update_url_id_provided_different_file_not_indexed(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting the same data again (with the id provided) and updating the
    URL field.

    HOWEVER the file hash and size in the new data do NOT match the previously
    submitted data for the given ID. The file hash/size provided does NOT
    match an already indexed file. e.g. The file is not yet indexed.

    The asssumption is that the user is attempting to UPDATE the index
    with a new file.

    FIXME At the moment, we do not allow updating like this
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = DEFAULT_UUID
    document.urls = [DEFAULT_URL]
    get_index_uuid.return_value = document

    # index yields no match given hash/size
    get_index_hash.return_value = None

    submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # now submit again but change url
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['id'] = DEFAULT_UUID
    updated_file['md5sum'] = DEFAULT_FILE_HASH.replace('0', '2')
    updated_file['file_size'] = DEFAULT_FILE_SIZE + 1
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=updated_file)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # make sure original url is still there and new url is NOT
    assert DEFAULT_URL in document.urls
    assert new_url not in document.urls

    # response
    assert_negative_response(resp)
    assert_single_entity_from_response(resp)


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_update_url_different_file_not_indexed(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting the different data (with NO id provided) and updating the
    URL field.

    HOWEVER the file hash and size in the new data do NOT match the previously
    submitted data. The file hash/size provided does NOT
    match an already indexed file. e.g. The file is not yet indexed.

    The asssumption is that the user is attempting to UPDATE the index
    with a new file but didn't provide a full id, just the same submitter_id
    as before.

    Without an ID provided, sheepdog falls back on secondary keys (being
    the submitter_id/project). There is already a match for that, BUT
    the provided file hash/size is different than the previously submitted one.
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document = MagicMock()
    document.did = DEFAULT_UUID
    document.urls = [DEFAULT_URL]
    get_index_uuid.return_value = document

    # index yields no match given hash/size
    get_index_hash.return_value = None

    submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # now submit again but change url
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['md5sum'] = DEFAULT_FILE_HASH.replace('0', '2')
    updated_file['file_size'] = DEFAULT_FILE_SIZE + 1
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=updated_file)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # make sure original url is still there and new url is NOT
    assert DEFAULT_URL in document.urls
    assert new_url not in document.urls

    # response
    assert_negative_response(resp)
    assert_single_entity_from_response(resp)


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_data_file_update_url_id_provided_different_file_already_indexed(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp):
    """
    Test submitting the same data again (with the id provided) and updating the
    URL field (should get added to the indexed file in index service).

    HOWEVER the file hash and size in the new data MATCH A DIFFERENT
    FILE in the index service that does NOT have the id provided.

    The asssumption is that the user is attempting to UPDATE the index
    with a new file they've already submitted under a different id.

    FIXME At the moment, we do not allow updating like this
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    document_with_id = MagicMock()
    document_with_id.did = DEFAULT_UUID
    document_with_id.urls = [DEFAULT_URL]

    different_file_matching_hash_and_size = MagicMock()
    different_file_matching_hash_and_size.did = '14fd1746-61bb-401a-96d2-342cfaf70000'
    different_file_matching_hash_and_size.urls = [DEFAULT_URL]

    get_index_uuid.return_value = document_with_id
    get_index_hash.return_value = different_file_matching_hash_and_size

    submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)

    # now submit again but change url
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['id'] = DEFAULT_UUID
    updated_file['md5sum'] = DEFAULT_FILE_HASH.replace('0', '2')
    updated_file['file_size'] = DEFAULT_FILE_SIZE + 1
    resp = submit_metadata_file(
        client, pg_driver, admin, submitter, cgci_blgsp, data=updated_file)

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # make sure original url is still there and new url is NOT
    assert DEFAULT_URL in document_with_id.urls
    assert DEFAULT_URL in different_file_matching_hash_and_size.urls
    assert new_url not in document_with_id.urls
    assert new_url not in different_file_matching_hash_and_size.urls

    # response
    assert_negative_response(resp)
    assert_single_entity_from_response(resp)


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index')
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias')
def test_create_file_no_required_index(create_alias, create_index, get_index_uuid, get_index_hash, client, pg_driver, admin, submitter, cgci_blgsp, require_index_exists_on):
    """
    With REQUIRE_FILE_INDEX_EXISTS = True.
    Test submitting a data file that does not exist in indexd (should raise an error and should not create an index or an alias).
    """
    submit_first_experiment(client, pg_driver, admin, submitter, cgci_blgsp)

    # no record in indexd for this file
    get_index_uuid.return_value = None
    get_index_hash.return_value = None

    # creating raises an error
    resp = submit_metadata_file(client, pg_driver, admin, submitter, cgci_blgsp)
    assert resp.status_code == 400

    # no index or alias creation
    assert not create_index.called
    assert not create_alias.called

    # response
    assert_negative_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'
