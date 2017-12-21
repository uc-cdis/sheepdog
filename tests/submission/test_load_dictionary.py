#import contextlib
import json
import os

#import boto
import pytest
from flask import g


def mock_request(f):
    def wrapper(*args, **kwargs):
        mock = mock_s3()
        mock.start(reset=False)
        conn = boto.connect_s3()
        conn.create_bucket('test_submission')

        result = f(*args, **kwargs)
        mock.stop()
        return result

    return wrapper


def put_cgci(client, auth=None, role='admin'):
    path = '/v0/submission'
    headers = auth(path, 'put', role) if auth else None
    data = json.dumps({
        'name': 'CGCI', 'type': 'program',
        'dbgap_accession_number': 'phs000235'
    })
    r = client.put(path, headers=headers, data=data)
    del g.user
    return r


def put_cgci_blgsp(client, auth=None, role='admin'):
    put_cgci(client, auth=auth, role=role)
    path = '/v0/submission/CGCI/'
    headers = auth(path, 'put', role) if auth else None
    data = json.dumps({
        "type": "project",
        "code": "BLGSP",
        "dbgap_accession_number": 'phs000527',
        "name": "Burkitt Lymphoma Genome Sequencing Project",
        "state": "open"
    })
    r = client.put(path, headers=headers, data=data)
    assert r.status_code == 200, r.data
    del g.user
    return r


def put_tcga_brca(client, submitter):
    headers = submitter('/v0/submission/', 'put', 'admin')
    data = json.dumps({
        'name': 'TCGA', 'type': 'program',
        'dbgap_accession_number': 'phs000178'
    })
    r = client.put('/v0/submission/', headers=headers, data=data)
    assert r.status_code == 200, r.data
    headers = submitter('/v0/submission/TCGA/', 'put', 'admin')
    data = json.dumps({
        "type": "project",
        "code": "BRCA",
        "name": "TEST",
        "dbgap_accession_number": "phs000178",
        "state": "open"
    })
    r = client.put('/v0/submission/TCGA/', headers=headers, data=data)
    assert r.status_code == 200, r.data
    del g.user
    return r


def test_program_creation_endpoint(client, pg_driver, submitter):
    resp = put_cgci(client, auth=submitter)
    assert resp.status_code == 200, resp.data
    print resp.data
    resp = client.get('/v0/submission/')
    assert resp.json['links'] == ['/v0/submission/CGCI'], resp.json


def test_program_creation_without_admin_token(client, pg_driver, submitter):
    path = '/v0/submission/'
    headers = submitter(path, 'put', 'member')
    data = json.dumps({'name': 'CGCI', 'type': 'program'})
    resp = client.put(path, headers=headers, data=data)
    assert resp.status_code == 403


# def test_program_creation_endpoint_for_program_not_supported(
#         client, pg_driver, submitter):
#     path = '/v0/submission/abc/'
#     resp = client.post(path, headers=submitter(path, 'post'))
#     assert resp.status_code == 404


# def test_project_creation_endpoint(client, pg_driver, submitter):
#     resp = put_cgci_blgsp(client, auth=submitter)
#     assert resp.status_code == 200
#     resp = client.get('/v0/submission/CGCI/')
#     with pg_driver.session_scope():
#         assert pg_driver.nodes(md.Project).count() == 1
#         n_cgci = (
#             pg_driver.nodes(md.Project)
#                 .path('programs')
#                 .props(name='CGCI')
#                 .count()
#         )
#         assert n_cgci == 1
#     assert resp.json['links'] == ['/v0/submission/CGCI/BLGSP'], resp.json