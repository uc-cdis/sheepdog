"""
Test upload entities (mostly data file handling and communication with
index service).
"""
import copy
import json
import os
import re
import uuid

import pytest

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

from gdcdatamodel.models import SubmittedAlignedReads, Case
from sheepdog.globals import UPDATABLE_FILE_STATES, RELEASED_NODE_STATES, MODIFIABLE_FILE_STATES
from sheepdog.transactions.upload.sub_entities import FileUploadEntity
from sheepdog.test_settings import SUBMISSION
from sheepdog.utils import (
    generate_s3_url,
    set_indexd_state,
    get_indexd)
from tests.integration.submission.test_versioning import release_indexd_doc
from tests.integration.submission.utils import (
    data_file_creation, read_json_data, put_entity_from_file
)
from tests.integration.submission.test_endpoints import DATA_DIR

PROGRAM = 'CGCI'
PROJECT = 'BLGSP'
BLGSP_PATH = '/v0/submission/{}/{}/'.format(PROGRAM, PROJECT)

# some default values for data file submissions
DEFAULT_FILE_HASH = '00000000000000000000000000000001'
DEFAULT_FILE_SIZE = 1
FILE_NAME = 'test-file'
DEFAULT_SUBMITTER_ID = '0'
DEFAULT_UUID = 'bef870b0-1d2a-4873-b0db-14994b2f89bd'
DEFAULT_URL = generate_s3_url(
    host=SUBMISSION['host'],
    bucket=SUBMISSION['bucket'],
    program=PROGRAM,
    project=PROJECT,
    uuid=DEFAULT_UUID,
    file_name=FILE_NAME,
)
# Regex because sometimes you don't get to upload a UUID, and the UUID is
# part of the s3 url.
UUID_REGEX = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
REGEX_URL = r's3://{}/{}/{}-{}/{}/{}'.format(
    SUBMISSION['host'],
    SUBMISSION['bucket'],
    PROGRAM,
    PROJECT,
    UUID_REGEX,
    FILE_NAME,
)

DEFAULT_METADATA_FILE = {
    'type': 'experimental_metadata',
    'data_type': 'Experimental Metadata',
    'file_name': FILE_NAME,
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


def submit_first_experiment(client, submitter):

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


def submit_metadata_file(client, admin, submitter, data=None):
    data = data or DEFAULT_METADATA_FILE
    data = json.dumps(data)
    resp = client.put(BLGSP_PATH, headers=submitter, data=data)
    return resp


def assert_alias_created(
        indexd_client, project_id='CGCI-BLGSP',
        submitter_id=DEFAULT_SUBMITTER_ID):
    alias = '{}/{}'.format(project_id, submitter_id)
    doc_by_alias = indexd_client.global_get(alias)
    assert doc_by_alias
    assert doc_by_alias.size == DEFAULT_FILE_SIZE
    assert doc_by_alias.hashes.get('md5') == DEFAULT_FILE_HASH


def assert_single_record(indexd_client):
    records = [r for r in indexd_client.list()]
    assert len(records) == 1
    return records[0]


def get_edges(node):
    """Return incoming and outgoing edges for a given node.

    NOTE: This method must be called within a session.
    """

    edges_in = {edge.src.node_id for edge in node.edges_in}
    edges_out = {edge.dst.node_id for edge in node.edges_out}

    return edges_in, edges_out


def test_data_file_not_indexed(
        client, pg_driver, admin, submitter, cgci_blgsp, indexd_client):
    """
    Test node and data file creation when neither exist and no ID is provided.
    """
    submit_first_experiment(client, submitter)

    resp = submit_metadata_file(client, admin, submitter)

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'

    indexd_doc = assert_single_record(indexd_client)

    # won't have an exact match because of the way URLs are generated
    # with a UUID in them
    assert re.match(REGEX_URL, indexd_doc.urls[0])

    # alias creation
    assert_alias_created(indexd_client)

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == indexd_doc.did


def test_data_file_not_indexed_id_provided(
        client, pg_driver, admin, submitter, cgci_blgsp, indexd_client):
    """
    Test node and data file creation when neither exist and an ID is provided.
    That ID should be used for the node and file index creation
    """

    submit_first_experiment(client, submitter)

    file = copy.deepcopy(DEFAULT_METADATA_FILE)
    file['id'] = DEFAULT_UUID
    resp = submit_metadata_file(
        client, admin, submitter, data=file)

    # response
    assert_positive_response(resp)
    entity = assert_single_entity_from_response(resp)
    assert entity['action'] == 'create'

    # indexd records
    indexd_doc = assert_single_record(indexd_client)

    # index creation
    assert indexd_doc.did == DEFAULT_UUID
    assert indexd_doc.hashes.get('md5') == DEFAULT_FILE_HASH
    assert DEFAULT_URL in indexd_doc.urls_metadata

    # alias creation
    assert_alias_created(indexd_client)


@pytest.mark.parametrize('id_provided', [False, True])
def test_data_file_already_indexed(
        id_provided,
        client, pg_driver, admin, submitter, cgci_blgsp, indexd_client):
    """
    Test submitting when the file is already indexed in the index client and
    1. ID is not provided. sheepdog should fall back on the hash/size of the
    file to find it in indexing service.
    2. ID is provided
    """
    submit_first_experiment(client, submitter)

    # submit metadata file once
    metadata_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    metadata_file['id'] = DEFAULT_UUID

    resp = submit_metadata_file(client, admin, submitter, data=metadata_file)
    record = assert_single_record(indexd_client)
    entity = assert_single_entity_from_response(resp)
    assert_positive_response(resp)
    assert entity['action'] == 'create'

    # submit same metadata file again (with or without id provided)
    metadata_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    if id_provided:
        metadata_file['id'] = entity['id']

    resp1 = submit_metadata_file(client, admin, submitter, data=metadata_file)
    record1 = assert_single_record(indexd_client)
    entity1 = assert_single_entity_from_response(resp1)
    assert_positive_response(resp1)
    assert entity1['action'] == 'update'

    # check that record did not change
    assert record.to_json() == record1.to_json()

    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == record1.did


@pytest.mark.parametrize('new_urls,id_provided', [
    (['some/new/url/location/to/add'], False),
    (['some/new/url/location/to/add'], True),
    ([DEFAULT_URL, 'some/new/url/location/to/add', 'some/other/url'], False),
    ([DEFAULT_URL, 'some/new/url/location/to/add', 'some/other/url'], True),
])
def test_data_file_update_urls(
        new_urls, id_provided,
        client, pg_driver, admin, submitter, cgci_blgsp, indexd_client):
    """
    Test submitting the same data again but updating the URL field (should
    get added to the indexed file in index service).
    """
    submit_first_experiment(client, submitter)

    # submit metadata_file once
    submit_metadata_file(client, admin, submitter)
    record = assert_single_record(indexd_client)

    # now submit again but change url
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = ','.join(new_urls)
    if id_provided:
        updated_file['id'] = record.did

    resp = submit_metadata_file(
        client, admin, submitter, data=updated_file)

    record1 = assert_single_record(indexd_client)
    entity = assert_single_entity_from_response(resp)
    assert_positive_response(resp)
    assert entity['action'] == 'update'
    # make sure uuid in node is the same as the uuid from index
    # FIXME this is a temporary solution so these tests will probably
    #       need to change in the future
    assert entity['id'] == record.did

    # make sure original url and new url are in the resulting document
    assert set(record1.urls) == set(record.urls) or set(new_urls)


@pytest.mark.config_toggle(
    parameters={
        'CREATE_REPLACEABLE': True
    }
)
def test_data_file_update_recreate_index(
        client_toggled, pg_driver, cgci_blgsp, indexd_client, submitter):
    resp_json, sur_entity = data_file_creation(
        client_toggled, submitter,
        sur_filename='submitted_unaligned_reads.json')

    assert resp_json['code'] == 201

    node_id = sur_entity['id']
    old_doc = indexd_client.get(node_id)

    new_version = read_json_data(
        os.path.join(DATA_DIR, 'submitted_unaligned_reads_new.json')
    )

    resp = client_toggled.put(
        BLGSP_PATH, headers=submitter, data=json.dumps(new_version))

    assert resp.status_code == 200

    new_doc_json = indexd_client.get(node_id).to_json()
    assert old_doc.to_json() != new_doc_json
    assert old_doc.to_json()['rev'] != new_doc_json['rev']
    assert new_doc_json['file_name'] == new_version['file_name']
    assert new_doc_json['hashes']['md5'] == new_version['md5sum']
    assert new_doc_json['size'] == new_version['file_size']


def test_is_updatable_file(client, pg_driver, indexd_client):
    """Test _is_updatable_file_node method
    """

    did = 'bef870b0-1d2a-4873-b0db-14994b2f89bd'
    url = '/some/url'

    # Create dummy file node and corresponding indexd record
    node = SubmittedAlignedReads(did)
    doc = indexd_client.create(
        did=did,
        urls=[url],
        hashes={'md5': '0'*32},
        size=1,
    )

    transaction = MagicMock()
    transaction.indexd = indexd_client
    entity = FileUploadEntity(transaction)
    entity.doc["md5sum"] = doc.hashes["md5"]
    entity.doc["file_size"] = doc.size
    entity.s3_url = url

    for file_state in UPDATABLE_FILE_STATES:
        # set node's url state in indexd
        indexd_doc = indexd_client.get(did)
        set_indexd_state(indexd_doc, url, file_state)

        # check if updatable
        entity._populate_file_exist_in_index()
        assert entity.is_updatable_file_node(node)


""" ----- TESTS THAT SHOULD RESULT IN SUBMISSION FAILURES ARE BELOW  ----- """


def test_data_file_update_url_invalid_id(
        client, pg_driver, admin, submitter, cgci_blgsp, indexd_client):
    """
    Test submitting the same data again (with the WRONG id provided).
    i.e. ID provided doesn't match the id from the index service for the file
         found with the hash/size provided

    FIXME: the 1:1 between node id and index/file id is temporary so this
           test may need to be modified in the future
    """
    submit_first_experiment(client, submitter)

    # submit metadata file once
    metadata_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    metadata_file['id'] = DEFAULT_UUID
    submit_metadata_file(client, admin, submitter, data=metadata_file)
    record = assert_single_record(indexd_client)

    # now submit again but change url and use wrong ID
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['id'] = DEFAULT_UUID.replace('1', '2')  # use wrong ID
    resp1 = submit_metadata_file(
        client, admin, submitter, data=updated_file)

    record1 = assert_single_record(indexd_client)

    # make sure it fails
    assert_negative_response(resp1)
    assert_single_entity_from_response(resp1)

    # make sure that indexd record did not change
    assert record.to_json() == record1.to_json()


@pytest.mark.parametrize('id_provided', [False, True])
def test_data_file_update_url_different_file_not_indexed(
        id_provided,
        client, pg_driver, admin, submitter, cgci_blgsp, indexd_client):
    """
    Test submitting the different data (with NO id provided) and updating the
    URL field.

    HOWEVER the file hash and size in the new data do NOT match the previously
    submitted data. The file hash/size provided does NOT
    match an already indexed file. e.g. The file is not yet indexed.

    The assumption is that the user is attempting to UPDATE the index
    with a new file but didn't provide a full id, just the same submitter_id
    as before.

    Without an ID provided, sheepdog falls back on secondary keys (being
    the submitter_id/project). There is already a match for that, BUT
    the provided file hash/size is different than the previously submitted one.
    """
    submit_first_experiment(client, submitter)

    # submit metadata file once
    metadata_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    metadata_file['id'] = DEFAULT_UUID
    submit_metadata_file(client, admin, submitter, data=metadata_file)
    record = assert_single_record(indexd_client)

    # now submit again but change url, hash and file size
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['md5sum'] = DEFAULT_FILE_HASH.replace('0', '2')
    updated_file['file_size'] = DEFAULT_FILE_SIZE + 1
    if id_provided:
        updated_file['id'] = DEFAULT_UUID

    resp1 = submit_metadata_file(
        client, admin, submitter, data=updated_file)
    record1 = assert_single_record(indexd_client)

    # make sure it fails
    assert_negative_response(resp1)
    assert_single_entity_from_response(resp1)

    # make sure that indexd record did not change
    assert record.to_json() == record1.to_json()


@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_hash') # noqa
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity.get_file_from_index_by_uuid') # noqa
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_index') # noqa
@patch('sheepdog.transactions.upload.sub_entities.FileUploadEntity._create_alias') # noqa
def test_data_file_update_url_id_provided_different_file_already_indexed(
        create_alias, create_index, get_index_uuid, get_index_hash,
        client, pg_driver, admin, submitter, cgci_blgsp, indexd_client):
    """
    Test submitting the same data again (with the id provided) and updating the
    URL field (should get added to the indexed file in index service).

    HOWEVER the file hash and size in the new data MATCH A DIFFERENT
    FILE in the index service that does NOT have the id provided.

    The assumption is that the user is attempting to UPDATE the index
    with a new file they've already submitted under a different id.

    FIXME At the moment, we do not allow updating like this
    """
    submit_first_experiment(client, submitter)

    document_with_id = MagicMock()
    document_with_id.did = DEFAULT_UUID
    document_with_id.urls = [DEFAULT_URL]

    different_file_matching_hash_and_size = MagicMock()
    different_file_matching_hash_and_size.did = '14fd1746-61bb-401a-96d2-342cfaf70000' # noqa
    different_file_matching_hash_and_size.urls = [DEFAULT_URL]

    get_index_uuid.return_value = document_with_id
    get_index_hash.return_value = different_file_matching_hash_and_size

    submit_metadata_file(client, admin, submitter)

    # now submit again but change url
    new_url = 'some/new/url/location/to/add'
    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['id'] = DEFAULT_UUID
    updated_file['md5sum'] = DEFAULT_FILE_HASH.replace('0', '2')
    updated_file['file_size'] = DEFAULT_FILE_SIZE + 1
    resp = submit_metadata_file(
        client, admin, submitter, data=updated_file)

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


@pytest.mark.config_toggle(
    parameters={
        'ENFORCE_FILE_HASH_SIZE_UNIQUENESS': False
    }
)
def test_dont_enforce_file_hash_size_uniqueness(
        client_toggled, pg_driver, admin, submitter, cgci_blgsp,
        indexd_client):
    """
    Check that able to submit two files with different did and urls but
    duplicate hash and size if ENFORCE_FILE_HASH_SIZE_UNIQUENESS set to False
    """

    submit_first_experiment(client_toggled, submitter)

    # submit metadata file once
    metadata_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    metadata_file['id'] = DEFAULT_UUID
    submit_metadata_file(client_toggled, admin, submitter, data=metadata_file)
    assert_single_record(indexd_client)

    release_indexd_doc(pg_driver, indexd_client, DEFAULT_UUID)
    # now submit again but change url and id
    new_id = DEFAULT_UUID.replace('0', '1')  # use different uuid
    new_url = 'some/new/url/location/to/add'

    updated_file = copy.deepcopy(DEFAULT_METADATA_FILE)
    updated_file['urls'] = new_url
    updated_file['id'] = new_id

    # release so that a new indexd document can be made
    submit_metadata_file(
        client_toggled, admin, submitter,
        data=updated_file,
    )

    # check that both are inserted into indexd and have correct urls
    records = [_ for _ in indexd_client.list()]
    assert len(records) == 2
    assert indexd_client.get(DEFAULT_UUID).urls == [DEFAULT_URL]
    assert indexd_client.get(new_id).urls == [new_url]


def test_cannot_update_node_in_state_submitted(
        client, pg_driver, indexd_client, submitter, cgci_blgsp):
    """Verify that indexd document is not updated after the node was submitted
    """
    _, sur_entity = data_file_creation(
        client, submitter, sur_filename='submitted_unaligned_reads.json')

    node_id = sur_entity['id']
    doc_original = indexd_client.get(node_id)

    # Set node state to 'submitted'
    with pg_driver.session_scope():
        sur_node = pg_driver.nodes().get(node_id)
        sur_node.state = 'submitted'

    resp = put_entity_from_file(
        client, os.path.join(DATA_DIR, 'submitted_unaligned_reads_new.json'),
        submitter, validate=False)

    doc_after_put = indexd_client.get(node_id)

    assert resp.status_code == 400, resp.json
    assert doc_original.to_json() == doc_after_put.to_json()


@pytest.mark.config_toggle(
    parameters={
        'CREATE_REPLACEABLE': True,
        'ENFORCE_FILE_HASH_SIZE_UNIQUENESS': False
    }
)
def test_cannot_update_node_if_file_is_validated(
        client_toggled, pg_driver, indexd_client, submitter, cgci_blgsp):
    _, sur_entity = data_file_creation(
        client_toggled, submitter,
        sur_filename='submitted_unaligned_reads.json')
    node_id = sur_entity['id']
    doc_original = indexd_client.get(node_id)

    # Set file state to 'validated', which should prevent the node update
    doc_original.urls_metadata[doc_original.urls[0]]['state'] = 'validated'
    doc_original.patch()

    resp = put_entity_from_file(
        client_toggled,
        os.path.join(DATA_DIR, 'submitted_unaligned_reads_new.json'),
        submitter, validate=False)

    doc_potentially_updated = indexd_client.get(doc_original.did)

    assert resp.status_code == 400, resp.json
    assert doc_original.to_json() == doc_potentially_updated.to_json()


@pytest.mark.config_toggle(
    parameters={
        'CREATE_REPLACEABLE': True,
    }
)
def test_update_multiple_one_fails(
        client_toggled, pg_driver, submitter, cgci_blgsp, indexd_client):
    """Test that updating multiple nodes at the same time with one of the nodes
    being in incorrect state will result in a failure of the whole update
    transaction, i.e. no nodes should be updated

    Test performs the following:
    1. Create several nodes + submitted unaligned reads node
    2. Create submitted aligned reads node
    3. Set submitted unaligned reads file state to "validated", which will
    prevent the node from being updated
    4. Send a request to update both aligned and unaligned reads nodes
    5. Verify that none of the nodes were updated
    """

    resp, sur_entity = data_file_creation(
        client_toggled, submitter,
        sur_filename='submitted_unaligned_reads.json')

    assert resp['code'] == 201

    sur_node_id = sur_entity['id']

    sar_old = read_json_data(
        os.path.join(DATA_DIR, 'submitted_aligned_reads.json'))

    # Create Submitted aligned reads file node
    resp = client_toggled.post(BLGSP_PATH, headers=submitter,
                               data=json.dumps(sar_old))
    assert resp.status_code == 201

    sar_node_id = resp.json['entities'][0]['id']

    sar_doc_old = indexd_client.get(sar_node_id).to_json()

    # Modify submitted aligned reads metadata
    sar_new = copy.deepcopy(sar_old)
    sar_new['file_name'] = 'submitted_aligned_reads_new.bam'
    sar_new['md5sum'] = 'ac4ca6d336b57a94b34e923d3d7a627a'
    sar_new['file_size'] = 12288

    # Load updated submitted unaligned reads metadata
    sur_new = read_json_data(
        os.path.join(DATA_DIR, 'submitted_unaligned_reads.json'))

    # Set unaligned reads file state to 'validated', which will prevent the
    # node update
    sur_doc = indexd_client.get(sur_node_id)
    sur_doc.urls_metadata[sur_doc.urls[0]]['state'] = 'validated'
    sur_doc.patch()

    sur_doc_old = sur_doc.to_json()

    data = [sar_new, sur_new]
    # The whole transaction should fail
    resp = client_toggled.put(BLGSP_PATH, headers=submitter,
                              data=json.dumps(data))

    # Should both contain old data
    sar_doc_new = indexd_client.get(sar_doc_old['did']).to_json()
    sur_doc_new = indexd_client.get(sur_node_id).to_json()

    assert resp.status_code == 400, resp.json
    assert sur_doc_old == sur_doc_new
    assert sar_doc_new == sar_doc_old


@pytest.mark.config_toggle(
    parameters={
        'CREATE_REPLACEABLE': True
    }
)
@pytest.mark.parametrize('released_state', RELEASED_NODE_STATES)
def test_update_released_non_file_node(
        client_toggled, pg_driver, submitter, cgci_blgsp, indexd_client,
        released_state, data_release):
    resp_json, sur_entity = data_file_creation(
        client_toggled, submitter,
        sur_filename='submitted_unaligned_reads.json')

    # Release submitted nodes
    with pg_driver.session_scope():
        for entity in resp_json['entities']:
            node = pg_driver.nodes().get(entity['id'])
            node.state = released_state

        node = pg_driver.nodes().get(sur_entity['id'])
        node.state = released_state

    # Load original case.json to validate the values
    case_json = read_json_data(
        os.path.join(DATA_DIR, 'case.json')
    )
    with pg_driver.session_scope():
        case_node_old = (
            pg_driver.nodes(Case).props(submitter_id=case_json['submitter_id'])
            .one()
        )
        assert case_node_old.primary_site == case_json.get('primary_site')

        # Save edges and then validate that they are still intact
        edges_in_old, edges_out_old = get_edges(case_node_old)

    # Update metadata of a case
    case_json['primary_site'] = 'Breast'
    response = client_toggled.put(
        BLGSP_PATH, headers=submitter, data=json.dumps(case_json))

    assert response.status_code == 200, response.json

    with pg_driver.session_scope():
        case_node_upd = (
            pg_driver.nodes(Case).props(submitter_id=case_json['submitter_id'])
            .one()
        )
        edges_in_new, edges_out_new = get_edges(case_node_upd)

        # Make sure that metadata updated successfully
        assert case_node_upd.primary_site == 'Breast'
        # Make sure that state hasn't changed
        assert case_node_upd.state == case_node_old.state
        # Make sure edges are preserved
        assert edges_in_old == edges_in_new
        assert edges_out_old == edges_out_new


@pytest.mark.config_toggle(
    parameters={
        'CREATE_REPLACEABLE': True
    }
)
@pytest.mark.parametrize('released_state', RELEASED_NODE_STATES)
def test_links_inherited_for_file_nodes(
        client_toggled, pg_driver, submitter, cgci_blgsp, indexd_client,
        released_state):
    resp_json, sar_entity = data_file_creation(
        client_toggled, submitter,
        sur_filename='submitted_aligned_reads.json'
    )

    additional_nodes = [
        'aligned_reads_index.json',
    ]

    meta_jsons = []
    for node_meta_filename in additional_nodes:
        meta_json = read_json_data(
            os.path.join(DATA_DIR, node_meta_filename))
        meta_jsons.append(meta_json)

    resp_2 = client_toggled.post(
        BLGSP_PATH, headers=submitter, data=json.dumps(meta_jsons))

    # Make sure that additional entities were created
    assert resp_2.status_code == 201

    # Release all of the nodes
    with pg_driver.session_scope():
        for entity in resp_json['entities'] + resp_2.json['entities']:
            pg_driver.nodes().get(entity['id']).state = released_state

        pg_driver.nodes().get(sar_entity['id']).state = released_state
    release_indexd_doc(pg_driver, indexd_client, sar_entity['id'], "10.2")

    # Save children edges for Submitted Aligned Reads file node
    with pg_driver.session_scope():
        sar_node = pg_driver.nodes().get(sar_entity['id'])

        edges_in_old, edges_out_old = get_edges(sar_node)

    # Update Submitted Aligned Reads file node payload
    sar_json = read_json_data(
        os.path.join(DATA_DIR, 'submitted_aligned_reads.json')
    )
    sar_json['file_name'] = 'submitted_aligned_reads_updated.bam'
    sar_json['file_size'] = 120489

    # Actual call to update Submitted Aligned Reads Node
    resp_upd = client_toggled.put(
        BLGSP_PATH, headers=submitter, data=json.dumps(sar_json))

    # Make sure update successful
    assert resp_upd.status_code == 200

    sar_id_upd = resp_upd.json['entities'][0]['id']

    # Fetch old and updated indexd docs
    indexd_old = indexd_client.get(sar_node.node_id)
    indexd_upd = indexd_client.get(sar_id_upd)

    # Verify that links have been recreated for the new node
    with pg_driver.session_scope():
        sar_node_upd = pg_driver.nodes().get(sar_id_upd)
        edges_in_new, edges_out_new = get_edges(sar_node_upd)

        # New node got created
        assert sar_node.node_id != sar_node_upd.node_id
        assert sar_node_upd.state == 'validated'
        # Indexd records are different and new info is there
        assert indexd_old.to_json() != indexd_upd.to_json()
        assert indexd_upd.file_name == sar_json['file_name']
        assert indexd_upd.size == sar_json['file_size']
        # Edges were relinked properly
        assert edges_in_new == edges_in_old
        assert edges_out_new == edges_out_old


@pytest.mark.config_toggle(
    parameters={
        'CREATE_REPLACEABLE': True
    }
)
def test_silently_update_released_node(data_release, released_file, client,
                                       admin, submitter, cgci_blgsp, indexd_client, pg_driver):

    submit_first_experiment(client, submitter)
    file_data = copy.deepcopy(DEFAULT_METADATA_FILE)
    file_data['id'] = released_file.did
    file_data["file_size"] = released_file.size
    file_data["md5sum"] = released_file.hashes["md5"]
    resp = submit_metadata_file(
        client, admin, submitter, data=file_data).json
    assert resp['code'] == 200

    entities = resp["entities"]
    assert len(entities) == 1

    version_id = entities[0]['id']
    with pg_driver.session_scope():
        node = pg_driver.nodes().get(version_id)
        assert node.data_format == file_data["data_format"]

    # assert no new version was created on indexd
    doc = indexd_client.get_latest_version(version_id)
    assert doc.version == "1"
    assert doc.metadata["release_number"] == data_release
