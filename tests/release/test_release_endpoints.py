"""
Tests for release endpoints
"""
import pytest


@pytest.mark.parametrize('header', (
    'submitter', 'admin'
))
def test_release_project_endpoint(header, client, request):
    """

    Args:
        client (requests):
        request(fixture):
    """
    headers = request.getfixturevalue(header)
    response = client.post('/v0/submission/GDC/INTERNAL/release', headers=headers)
    # print(response)


def test_release_project_dry_run_endpoint():
    pass
