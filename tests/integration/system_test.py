
def test_status_endpoint(client):
    res = client.get("/_status")
    assert res.status_code == 200

def test_version_endpoint(client):
    res = client.get("/_version")
    assert res.status_code == 200
