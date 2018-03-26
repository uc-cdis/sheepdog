"""
Tests for admin endpoint functionality.
"""
# pylint: disable=unused-argument, no-member

import json
import datetime

import pytest

from gdcdatamodel import models as md
from tests.submission.test_endpoints import post_example_entities_together
from tests.submission.utils import data_fnames


PREVIOUS_RELEASES = [(1, 1), (1, 2), (1, 5)]


class Releaser:
    """
    Helper functions for release testing
    """

    @classmethod
    def make_release_history(cls, client, headers):
        """
        Create three valid previous releases
        """
        for i, (major, minor) in enumerate(PREVIOUS_RELEASES):
            # Create release node
            cls.create_release(client, headers, major, minor)

            # Set release node to released and add release_date
            release_date = str(datetime.date.today() +
                               datetime.timedelta(days=-3+i))
            cls.set_release(client, headers,
                            {'released': True, 'release_date': release_date})

    @staticmethod
    def create_release(client, headers, major_version, minor_version):
        """
        Call /v0/submission/admin/release/create to create a release candidate node
        """
        return client.post(
            '/v0/submission/admin/release/create', headers=headers,
            data=json.dumps({'major_version': major_version,
                             'minor_version': minor_version})
        )

    @staticmethod
    def set_release(client, headers, params):
        """
        Call /v0/submission/admin/release/set to set release candidate node
        """
        return client.put(
            '/v0/submission/admin/release/set', headers=headers,
            data=json.dumps(params)
        )

    @staticmethod
    def get_release(client, headers, return_all=False):
        """
        Call /v0/submission/admin/release/get to get release candidate node[s]
        """
        url = '/v0/submission/admin/release/get'
        if return_all:
            url = url + '?all=True'
        return client.get(
            url, headers=headers,
        )


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


@pytest.mark.parametrize("headers,status_code,to_delete", [
    ('submitter', 403, None),
    ('admin', 200, True),
    ('admin', 200, False),
])
def test_to_delete(headers, status_code, to_delete, request, client,
                   pg_driver, cgci_blgsp, submitter):
    """Try to set the sysan of a node with admin credentials

    Url:
        DELETE: /admin/<program>/<project>/entities/<ids>/to_delete/<to_delete>
    """

    headers = request.getfixturevalue(headers)

    # submit files as submitter
    entities = post_blgsp_files(client, submitter)
    did = entities['submitted_unaligned_reads']

    base_delete_path = create_blgsp_url('/entities/{}/to_delete/'.format(did))
    to_delete_path = base_delete_path + str(to_delete).lower()

    resp = client.delete(to_delete_path, headers=headers)
    assert resp.status_code == status_code, resp.data
    with pg_driver.session_scope():
        sur_node = pg_driver.nodes(md.SubmittedUnalignedReads).first()
        assert sur_node
        assert sur_node.sysan.get('to_delete') is to_delete


def test_create_release_node(client, pg_driver, submitter, admin):
    """
    test DataRelease node creation endpoint
    """
    # insert previous releases
    Releaser.make_release_history(client, admin)

    # can not create when not admin
    resp = Releaser.create_release(client, submitter, 2, 0)
    assert resp.status_code == 403
    assert "You don't have admin access" in resp.json['message']

    # can not jump major versions
    resp = Releaser.create_release(client, admin, 3, 0)
    assert resp.status_code == 400
    assert 'Can not increase major version by more then one' in resp.json['message']

    # can not create older versions
    for major, minor in [(0, 0), (1, 3), (1, 5)]:
        resp = Releaser.create_release(client, admin, major, minor)
        assert resp.status_code == 400
        assert 'Release version must be higher' in resp.json['message']

    # can create valid version
    resp = Releaser.create_release(client, admin, 2, 6)
    assert resp.status_code == 200
    assert 'DataRelease node created' in resp.json['message']

    # can not create second unreleased node
    resp = Releaser.create_release(client, admin, 2, 8)
    assert resp.status_code == 400
    assert 'Can not create release candidate' in resp.json['message']


def test_get_release_node(client, pg_driver, submitter, admin):
    """
    test DataRelease node retrieval endpoint
    """
    # insert previous releases
    Releaser.make_release_history(client, admin)

    # can not get when not admin
    resp = Releaser.get_release(client, submitter)
    assert resp.status_code == 403

    # no release candidates found
    resp = Releaser.get_release(client, admin)
    assert resp.status_code == 200
    assert resp.json == {}

    # create release candidate v2.0 and get it
    Releaser.create_release(client, admin, 2, 0)
    resp = Releaser.get_release(client, admin)
    assert resp.status_code == 200
    assert resp.json['major_version'], resp.json['minor_version'] == (2, 0)

    # get and verify release history
    resp = Releaser.get_release(client, admin, return_all=True)
    assert resp.status_code == 200
    release_history = [
        (r['major_version'], r['minor_version']) for r in resp.json['release_history']
    ]
    assert release_history == (PREVIOUS_RELEASES + [(2, 0)])[::-1]


def test_set_release_node(client, pg_driver, submitter, admin):
    """
    test DataRelease node parameter setting endpoint
    """
    # insert previous releases
    Releaser.make_release_history(client, admin)

    valid_params = {'release_date': str(datetime.date.today()), 'released': True}

    # try to set when no release candidate
    resp = Releaser.set_release(client, admin, valid_params)
    assert resp.status_code == 400
    assert 'No release candidate found' in resp.json['message']

    # create release candidate v2.0
    Releaser.create_release(client, admin, 2, 0)

    # can not set when not admin
    resp = Releaser.set_release(client, submitter, valid_params)
    assert resp.status_code == 403

    # try to set with nothing
    resp = Releaser.set_release(client, admin, {})
    assert resp.status_code == 400
    assert 'Nothing to set' in resp.json['message']

    # try to set with invalid release_date
    resp = Releaser.set_release(
        client, admin,
        {'release_date': str(datetime.date.today() - datetime.timedelta(days=2))}
    )
    assert resp.status_code == 400
    assert 'Invalid release date' in resp.json['message']

    # try valid setting
    resp = Releaser.set_release(client, admin, valid_params)
    assert resp.status_code == 200
    assert 'Successfully updated'

    # try to set after candidate is released
    resp = Releaser.set_release(client, admin, {'released': False})
    assert resp.status_code == 400
    assert 'No release candidate found' in resp.json['message']


def do_reassign(client, headers):
    """Perform the http reassign action

    Args:
        client (pytest.Fixture): Allows you to mock http requests through flask
        headers (dict): http headers with token

    Returns:
        requests.Response: http response from doing reassign request
        string: did
        string: s3 url that you changed it to
    """

    entities = post_blgsp_files(client, headers)
    did = entities['submitted_unaligned_reads']
    s3_url = 's3://whatever/you/want'

    reassign_path = create_blgsp_url('/files/{}/reassign'.format(did))
    data = json.dumps({'s3_url': s3_url})

    return client.put(reassign_path, headers=headers, data=data), did, s3_url


def test_reassign_with_admin(client, pg_driver, cgci_blgsp, submitter, index_client, admin):
    """Try to reassign a node's remote URL

    Url:
        PUT: /admin/<program>/<project>/files/<file_uuid>/reassign
        data: {"s3_url": "s3://whatever/you/want"}
    """

    resp, did, s3_url = do_reassign(client, admin)
    assert resp.status_code == 200, resp.data
    assert index_client.get(did), 'Did not register with indexd?'
    assert s3_url in index_client.get(did).urls, 'Did not successfully reassign'


def test_reassign_without_admin(client, pg_driver, cgci_blgsp, submitter, index_client):
    """Try to reassign a node's remote URL

    Url:
        PUT: /admin/<program>/<project>/files/<file_uuid>/reassign
        data: {"s3_url": "s3://whatever/you/want"}
    """

    resp, did, s3_url = do_reassign(client, submitter)
    assert resp.status_code == 403, resp.data
    assert index_client.get(did), 'Index should have been created.'
    assert len(index_client.get(did).urls) == 0, 'No files have been uploaded'
    assert s3_url not in index_client.get(did).urls, 'Should not have reassigned'
