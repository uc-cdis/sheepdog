import datetime
import json
import re
import urlparse

import httmock
import pytest

from sheepdog.auth import AuthDriver
from sheepdog import test_settings


class Config(object):
    ADMIN_USERNAME = 'iama_username'
    ADMIN_PASSWORD = 'iama_password'

    ADMIN_TOKEN = 'admin_token'
    MEMBER_TOKEN = 'member_token'
    SUBMITTER_TOKEN_A = 'submitter_a'
    SUBMITTER_TOKEN_B = 'submitter_b'
    SUBMITTER_ADMIN_TOKEN = 'submitter_c'
    SUBMITTER_MEMBER_TOKEN = 'submitter_member'
    SUBMITTER_DOWNLOAD_TOKEN = 'submitter_download'
    SUBMITTER_TOKENS = [SUBMITTER_TOKEN_A, SUBMITTER_TOKEN_B,
                        SUBMITTER_ADMIN_TOKEN,
                        SUBMITTER_MEMBER_TOKEN,
                        SUBMITTER_DOWNLOAD_TOKEN]
    VALID_TOKENS = [MEMBER_TOKEN] + SUBMITTER_TOKENS
    MEMBERS = [MEMBER_TOKEN, SUBMITTER_MEMBER_TOKEN]
    DOWNLOADERS = [SUBMITTER_DOWNLOAD_TOKEN]
    SUBMITTERS = [SUBMITTER_TOKEN_A, SUBMITTER_TOKEN_B, SUBMITTER_ADMIN_TOKEN]
    INVALID_TOKEN = 'IMNOTVALID'
    # Token that will trigger 500 in GET /auth/token
    FAIL_TOKEN = 'FAIL'
    PROJECT = 'projectA'
    PROJECTS = {SUBMITTER_TOKEN_A: ['phs000178'],
                SUBMITTER_TOKEN_B: ['phs000235'],
                SUBMITTER_DOWNLOAD_TOKEN: ['phs000235'],
                SUBMITTER_ADMIN_TOKEN: ['phs000178', 'phs000235', 'phs000218'],
                SUBMITTER_MEMBER_TOKEN: ['phs000178', 'phs000235', 'phs000218'],
                MEMBER_TOKEN: ['projectA']}
    ROLES = ['_member_', 'create', 'delete', 'update', 'read',
             'download', 'release', 'read_report']

endpoint = 'https://fake_auth_url'
admin_token_resp = {
    "token":
        {"domain": {"id": "abc", "name": "admin"},
         "methods": ["token", "password"],
         "expires_at": (datetime.datetime.now() +
                        datetime.timedelta(hours=24*365)).isoformat("T")+'Z',
         "catalog":
            [{"endpoints":
                [{"url": endpoint, "interface": "public", "region": None, "region_id": None, "id": ""},
                 {"url": endpoint, "interface": "internal", "region": None, "region_id": None, "id": ""},
                 {"url": endpoint, "interface": "admin", "region": None, "region_id": None, "id": ""}],
              "type": "identity",
              "id": "",
              "name": "admin"}],
         "extras": {},
         'roles': [{'id': 'admin', 'name': 'admin'}],
         "user": {"domain": {"id": "", "name": "admin"},
                  "id": "abc", "name": "admin"},
         "audit_ids": ["-SfXoGA"],
         "issued_at": "2015-09-01T19:09:40.916497Z"}}


headers = {
    'Content-Type': 'application/json',
}


@httmock.urlmatch(path=r'.*/user-projects/$', method='GET')
def get_user_projects(url, req):
    auth_token = req.headers.get('X-Auth-Token')
    if auth_token == Config.ADMIN_TOKEN:
        return httmock.response(401)
    elif auth_token == Config.FAIL_TOKEN:
        return httmock.response(500)
    else:
        projects = Config.PROJECTS.get(auth_token)

        if auth_token in Config.MEMBERS:
            roles = ['_member_']
        elif auth_token in Config.DOWNLOADERS:
            roles = ['_member_', 'download']
        elif auth_token in Config.SUBMITTERS:
            roles = Config.ROLES
        else:
            return httmock.response(403)
        return httmock.response(
            200, json.dumps({project:roles for project in projects}),
            headers = {'Content-Type': 'application/json'})


@httmock.urlmatch(path=r'.*/auth/tokens$', method='GET')
def get_auth_tokens(url, req):

    # TODO populate with other portions of response

    auth_token = req.headers.get('X-Auth-Token')
    if auth_token != Config.ADMIN_TOKEN:
        return httmock.response(401)

    subject_token = req.headers.get('X-Subject-Token')
    if subject_token == Config.FAIL_TOKEN:
        return httmock.response(500)

    headers = {
        'Content-Type': 'application/json',
        'X-Subject-Token': subject_token,
    }

    if subject_token in Config.VALID_TOKENS:
        valid_token_resp = {
            "token": {"methods": ["saml2"],
                      "expires_at": "2015-10-01T18:04:56.623912Z",
                      "extras": {},
                      "user": {"OS-FEDERATION":
                                {"identity_provider": {"id": "era_common"},
                                 "protocol": {"id": "saml2"},
                                 "groups": [{"id": subject_token}]},
                               "id": subject_token,
                               "name": subject_token},
                      "audit_ids": ["abc"],
                      "issued_at": "2015-09-01T18:04:56.624021Z"}}

        return httmock.response(200, json.dumps(valid_token_resp), headers)
    elif subject_token == Config.ADMIN_TOKEN:
        return httmock.response(200, json.dumps(admin_token_resp), headers)
    else:
        return httmock.response(404)


@httmock.urlmatch(path=r'.*/auth/tokens$', method='POST')
def post_auth_tokens(url, req):

    # TODO populate with other portions of response
    body = admin_token_resp
    headers = {
        'content-type': 'application/json',
        'x-subject-token': Config.ADMIN_TOKEN,
    }
    return httmock.response(201, json.dumps(body), headers)


@httmock.urlmatch(path=r'.*/auth/tokens$', method='HEAD')
def head_auth_tokens(url, req):
    # TODO populate with other portions of response

    if req.headers.get('X-Auth-Token') != Config.ADMIN_TOKEN:
        return httmock.response(401)

    if req.headers.get('X-Subject-Token') not in Config.VALID_TOKENS:
        return httmock.response(404)

    return httmock.response(200)


@httmock.urlmatch(path=r'.*/auth/tokens$', method='DELETE')
def delete_auth_tokens(url, req):

    # TODO populate with other portions of response

    if req.headers.get('X-Auth-Token') != Config.ADMIN_TOKEN:
        return httmock.response(401)

    if req.headers.get('X-Subject-Token') not in Config.VALID_TOKENS:
        return httmock.response(404)

    return httmock.response(204)


@httmock.urlmatch(path=r'.*/OS-FEDERATION/projects$', method='GET')
def get_os_federation_projects(url, req):
    # TODO populate with other portions of response

    if req.headers.get('X-Auth-Token') not in\
            Config.VALID_TOKENS + [Config.FAIL_TOKEN]:
        return httmock.response(404)

    body = {
        'projects': [{
            'name': Config.PROJECT,
        }],
    }

    return httmock.response(200, json.dumps(body), headers)


@httmock.urlmatch(path=r'.*/groups.*$', method='GET')
def list_groups(url, req):
    group = urlparse.parse_qs(url.query).get('name')
    if group:
        group = group[0]
        if group in Config.VALID_TOKENS:
            body = {"groups": [_contruct_group(group)]}
            return httmock.response(200, json.dumps(body), headers)
    return httmock.response(404)


@httmock.urlmatch(path=r'.*/groups/.*$', method='GET')
def get_group(url, req):
    matcher = re.compile("/groups/(?P<group>\w+)")
    args = matcher.match(url.path).groupdict()
    group = args['group']
    if group in Config.VALID_TOKENS:
        body = {"group": _contruct_group(group)}
        return httmock.response(200, json.dumps(body), headers)
    else:
        return httmock.response(404)


def _contruct_group(group):
    return {'description': 'test group',
            'domain_id': 'abc',
            'id': group,
            'links': {
                'self': endpoint + '/groups/' + group,
                },
            'name': group
            }


@httmock.urlmatch(path=r'.*/projects$', method='GET')
def find_projects(url, req):
    project = urlparse.parse_qs(url.query).get('name')
    if project is not None:
        project = project[0]
        body = {
            'projects': [
                {'domain_id': 'abc',
                 'parent_id': 'abc',
                 'enabled': True,
                 'id': project,
                 'links': {
                     'self': endpoint + '/projects/' + project},
                 'name': project}]}

        return httmock.response(200, json.dumps(body), headers)

    return httmock.response(404)


@httmock.urlmatch(path=r'.*/projects/.*/groups/.*/roles/.*')
def check_role(url, req):
    matcher = re.compile(
        "/projects/(?P<project>\w+)/groups/(?P<group>\w+)/roles/(?P<role>\w+)")
    args = matcher.match(url.path).groupdict()
    token = args['group']
    if token in Config.VALID_TOKENS:
        if args['project'] in Config.PROJECTS[token] and \
                args['role'] in Config.ROLES:
            if token == Config.SUBMITTER_MEMBER_TOKEN and \
                    args['role'] != '_member_':
                return httmock.response(401)
            elif token == Config.SUBMITTER_DOWNLOAD_TOKEN and \
                    args['role'] not in ['_member_', 'download']:
                return httmock.response(401)
            return httmock.response(204)
        else:
            return httmock.response(401)
    return httmock.response(404)


@httmock.urlmatch(path=r'.*/roles$', method='GET')
def find_roles(url, req):
    role = urlparse.parse_qs(url.query).get('name')
    if role is not None:
        role = role[0]
        if role in Config.ROLES:
            body = {
                'roles': [
                    {'id': role,
                     'links': {
                         'self': endpoint + '/roles/' + role},
                     'name': role}]}

            return httmock.response(200, json.dumps(body), headers)
    else:
        body = {'roles':
                [{"id": role,
                  "links": {"self": endpoint + "/roles/" + role},
                  "name": role} for role in Config.ROLES]}
        return httmock.response(200, json.dumps(body), headers)

    return httmock.response(404)


@httmock.all_requests
def default(url, request):
    return httmock.response(404)


def mock(*args, **kwargs):
    '''
    Generates a new mock using the endpoints defined with additional endpoints
    added to the front of the resolution order.
    '''
    endpoints = list(args) + [
        get_auth_tokens,
        post_auth_tokens,
        head_auth_tokens,
        delete_auth_tokens,
        get_os_federation_projects,
        get_group,
        list_groups,
        find_projects,
        find_roles,
        check_role,
        get_user_projects,
        # NOTE default should always be last - default behavior
        default,
    ]
    return httmock.HTTMock(*endpoints)


@pytest.fixture
def auth(request):
    with mock():
        driver = AuthDriver(test_settings.AUTH_ADMIN_CREDS, test_settings.INTERNAL_AUTH)
    driver.validate = patch_auth(driver.validate)
    driver.get_keystone_user = patch_auth(driver.get_keystone_user)
    driver.get_user_projects = patch_auth(driver.get_user_projects)

    return driver


def patch_auth(f):
    def wrapper(*args, **kwargs):
        with mock():
            return f(*args, **kwargs)
    return wrapper
