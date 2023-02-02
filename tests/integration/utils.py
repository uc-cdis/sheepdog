import json
import requests

from flask import g


def get_parent(path):
    print(path)
    return path[0 : path.rfind("/")]


def wait_for_indexd_alive(port):
    url = "http://localhost:{}/_status".format(port)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return wait_for_indexd_alive(port)
    else:
        return


def wait_for_indexd_not_alive(port):
    url = "http://localhost:{}/_status".format(port)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return
    else:
        return wait_for_indexd_not_alive(port)


def put_cgci(client, auth=None):
    path = "/v0/submission"
    headers = auth
    data = json.dumps(
        {
            "name": "CGCI",
            "type": "program",
            "dbgap_accession_number": "phs000235",
        }
    )
    r = client.put(path, headers=headers, data=data)
    return r


def put_cgci2(client, auth=None):
    path = "/v0/submission"
    headers = auth
    data = json.dumps(
        {"name": "CGCI2", "type": "program", "dbgap_accession_number": "phs0002352"}
    )
    r = client.put(path, headers=headers, data=data)
    return r


def put_cgci_blgsp(client, auth=None):
    r = put_cgci(client, auth=auth)
    assert r.status_code == 200, r.data

    path = "/v0/submission/CGCI/"
    headers = auth
    data = json.dumps(
        {
            "type": "project",
            "code": "BLGSP",
            "dbgap_accession_number": "phs000527",
            "name": "Burkitt Lymphoma Genome Sequencing Project",
            "state": "open",
        }
    )
    r = client.put(path, headers=headers, data=data)
    assert r.status_code == 200, r.data
    del g.user
    return r


def put_tcga_brca(client, submitter):
    headers = submitter
    data = json.dumps(
        {"name": "TCGA", "type": "program", "dbgap_accession_number": "phs000178"}
    )
    r = client.put("/v0/submission/", headers=headers, data=data)
    assert r.status_code == 200, r.data
    headers = submitter
    data = json.dumps(
        {
            "type": "project",
            "code": "BRCA",
            "name": "TEST",
            "dbgap_accession_number": "phs000178",
            "state": "open",
        }
    )
    r = client.put("/v0/submission/TCGA/", headers=headers, data=data)
    assert r.status_code == 200, r.data
    del g.user
    return r
