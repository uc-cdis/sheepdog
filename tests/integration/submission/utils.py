import json
import os
import re
import uuid
import indexclient

from gdcdatamodel import models
from psqlgraph import Node

DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

# https://stackoverflow.com/questions/373194/python-regex-for-md5-hash
re_md5 = re.compile(r'(i?)(?<![a-z0-9])[a-f0-9]{32}(?![a-z0-9])')

DATA_FILES = [
    'experiment.json',
    'case.json',
    'sample.json',
    'aliquot.json',
    'demographic.json',
    'diagnosis.json',
    'exposure.json',
    'treatment.json',
]

PATH = '/v0/submission/graphql'
BLGSP_PATH = '/v0/submission/CGCI/BLGSP/'
BRCA_PATH = '/v0/submission/TCGA/BRCA/'


def put_entity_from_file(
        client, file_path, submitter, put_path=BLGSP_PATH, validate=True):
    with open(os.path.join(DATA_DIR, file_path), 'r') as f:
        entity = f.read()
    r = client.put(put_path, headers=submitter, data=entity)
    if validate:
        assert r.status_code == 200, r.data
    return r


def reset_transactions(pg_driver):
    with pg_driver.session_scope() as s:
        s.query(models.submission.TransactionSnapshot).delete()
        s.query(models.submission.TransactionDocument).delete()
        s.query(models.submission.TransactionLog).delete()


def patch_indexclient(monkeypatch):

    called = {'create': False, 'create_alias': False}

    def check_hashes(hashes):
        assert hashes is not None
        assert 'md5' in hashes
        assert re_md5.match(hashes['md5']) is not None

    def check_uuid4(self, did=None, urls=None, hashes=None, size=None, metadata=None, file_name=None):
        """
        Using code from: https://gist.github.com/ShawnMilo/7777304
        """
        called['create'] = True
        # Check for valid UUID.
        try:
            val = uuid.UUID(did, version=4)
            assert val.hex == did.replace('-', '')
        except Exception:
            raise AssertionError('invalid uuid')
        check_hashes(hashes)

    def check_alias(
            self, record, size=None, hashes=None, release=None,
            metastring=None, host_authorities=None, keeper_authority=None):
        called['create_alias'] = True
        check_hashes(hashes)
        assert isinstance(record, str)

    monkeypatch.setattr(
        indexclient.client.IndexClient, 'create', check_uuid4
    )
    monkeypatch.setattr(
        indexclient.client.IndexClient, 'create_alias', check_alias
    )
    return called


def assert_positive_response(resp):
    assert resp.status_code == 200, resp.data
    entities = resp.json['entities']
    for entity in entities:
        assert not entity['errors']
    assert resp.json['success'] is True


def assert_negative_response(resp):
    assert resp.status_code != 200, resp.data
    entities = resp.json['entities']

    # check if at least one entity has an error
    entity_errors = [entity['errors'] for entity in entities if entity['errors']]
    assert entity_errors

    assert resp.json['success'] is False


def assert_single_entity_from_response(resp):
    entities = resp.json['entities']
    assert len(entities) == 1
    return entities[0]


def post_example_entities_together(client, submitter, data_fnames=None,
                                   dry_run=False, file_size=None):
    if not data_fnames:
        data_fnames = DATA_FILES
    path = BLGSP_PATH
    if dry_run:
        path = os.path.join(BLGSP_PATH, '_dry_run')

    data = []
    for fname in data_fnames:
        with open(os.path.join(DATA_DIR, fname), 'r') as f:
            node_data = json.loads(f.read())
            if file_size and is_file_node_label(node_data["type"]):
                node_data["file_size"] = file_size
            data.append(node_data)
    return client.post(path, headers=submitter, data=json.dumps(data))


def put_example_entities_together(client, headers, data_fnames=None,
                                  dry_run=False, file_size=None):
    if not data_fnames:
        data_fnames = DATA_FILES
    path = BLGSP_PATH
    if dry_run:
        path = os.path.join(path, '_dry_run')

    data = []
    for fname in data_fnames:
        with open(os.path.join(DATA_DIR, fname), 'r') as f:
            node_data = json.loads(f.read())
            if file_size and is_file_node_label(node_data["type"]):
                node_data["file_size"] = file_size
            data.append(node_data)
    return client.put(path, headers=headers, data=json.dumps(data))


def read_json_data(filepath):
    with open(filepath, 'r') as f:
        json_data = json.loads(f.read())
    return json_data


def is_file_node_label(label):
    """Checks if the given node is of the file category
    Args:
        label (str): node label
    Returns:
        bool: True is ;abel represents a file node
    """
    label_cls = Node.get_subclass(label)
    node_category = label_cls._dictionary.get("category")  # type: str
    return node_category and node_category.endswith("_file")


def data_file_creation(client, headers, method='post', sur_filename='',
                       dry_run=False, file_size=None):
    """
    Boilerplate setup code for some tests

    Submit nodes for creation and get back the data file associated
    with the submission.

    Args:
        client (fixture): fixture for making http requests
        headers (dict): http header with token
        method (string): HTTP PUT or POST
        sur_filename (str): filename to use for the submitted unaligned reads file
        file_size (int): used to updaye the file size before sending
    Returns:
        tuple (dict, pytest_flask.plugin.JSONResponse): file metadata dict and
            sheepdog response
    """

    if method == 'post':
        upload_function = post_example_entities_together
    elif method == 'put':
        upload_function = put_example_entities_together

    test_fnames = DATA_FILES + ['read_group.json']
    resp = upload_function(client,
                           headers,
                           data_fnames=test_fnames + [sur_filename],
                           dry_run=dry_run, file_size=file_size)

    assert_message = 'Unable to create nodes: {}'.format(
        [entity for entity in resp.json['entities'] if entity['errors']]
    )
    assert resp.status_code in (200, 201), assert_message

    for entity in resp.json['entities']:
        if entity['type'] in ('submitted_unaligned_reads', 'submitted_aligned_reads'):
            sur_entity = entity

    return resp.json, sur_entity


def release_indexd_doc(pg_driver, indexd_client, latest_did):
    """Simulate a released node in indexd

    Args:
        pg_driver (pytest fixture): client connected to postgres server
        indexd_client (pytest fixture): client connected to indexd server
        latest_did (str): did of a document in indexd
    """

    indexd_doc = indexd_client.get(latest_did)

    # make the url state (file_state) validated
    for url in indexd_doc.urls_metadata:
        indexd_doc.urls_metadata[url]['state'] = 'validated'

    # change node state to released
    with pg_driver.session_scope():
        pg_driver.nodes().get(latest_did).state = 'released'

    # version the rest of the nodes
    docs = indexd_client.list_versions(latest_did)

    # create newest version number
    new_version = int(max([d.version for d in docs]) or '0') + 1
    indexd_doc.version = str(new_version)
    indexd_doc.patch()
