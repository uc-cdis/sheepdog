#!/usr/bin/env python

from collections import defaultdict
import os

from authutils import ROLES as all_roles
from flask import current_app
from mock import patch, PropertyMock
from psqlgraph import PolyNode as Node
import requests

from sheepdog.api import run_for_development


requests.packages.urllib3.disable_warnings()

all_role_values = list(all_roles.values())
roles = defaultdict(lambda: all_role_values)


class FakeBotoKey(object):
    def __init__(self, name):
        self.name = name

    def close(self):
        pass

    def open_read(self, *args, **kwargs):
        pass

    @property
    def size(self):
        return len("fake data for {}".format(self.name))

    def __iter__(self):
        for string in ["fake ", "data ", "for ", self.name]:
            yield string


def fake_get_nodes(dids):
    nodes = []
    for did in dids:
        try:
            file_name = files.get(did, {})["data"]["file_name"]
        except ValueError:
            file_name = did
        nodes.append(
            Node(
                node_id=did,
                label="file",
                acl=["open"],
                properties={
                    "file_name": file_name,
                    "file_size": len("fake data for {}".format(did)),
                    "md5sum": "fake_md5sum",
                    "state": "live",
                },
            )
        )
    return nodes


def fake_urls_from_index_client(did):
    return ["s3://fake-host/fake_bucket/{}".format(did)]


def fake_key_for(parsed):
    return FakeBotoKey(parsed.netloc.split("/")[-1])


def fake_key_for_node(node):
    return FakeBotoKey(node.node_id)


class FakeUser(object):
    username = "test"
    roles = roles


def set_user(*args, **kwargs):
    from flask import g

    g.user = FakeUser()


def run_with_fake_auth():
    def get_project_ids(role="_member_", project_ids=None):
        from gen3datamodel import models as md

        if project_ids is None:
            project_ids = []
        if not project_ids:
            with current_app.db.session_scope():
                project_ids += [
                    "{}-{}".format(p.programs[0].name, p.code)
                    for p in current_app.db.nodes(md.Project).all()
                ]
        return project_ids

    with patch(
        "sheepdog.auth.FederatedUser.roles",
        new_callable=PropertyMock,
        return_value=roles,
    ), patch(
        "sheepdog.auth.FederatedUser.logged_in",
        new_callable=PropertyMock,
        return_value=lambda: True,
    ), patch(
        "sheepdog.auth.FederatedUser.get_project_ids",
        new_callable=PropertyMock,
        return_value=get_project_ids,
    ), patch(
        "sheepdog.auth.verify_hmac", new=set_user
    ):

        run_for_development(debug=debug, threaded=True)


def run_with_fake_authz():
    """
    Mocks arborist calls.
    """
    authorized = True  # modify this to mock authorized/unauthorized
    with patch(
        "gen3authz.client.arborist.client.ArboristClient.create_resource",
        new_callable=PropertyMock,
    ), patch(
        "gen3authz.client.arborist.client.ArboristClient.auth_request",
        new_callable=PropertyMock,
        return_value=lambda jwt, service, methods, resources: authorized,
    ):
        run_for_development(debug=debug, threaded=True)


def run_with_fake_download():
    with patch("sheepdog.download.get_nodes", fake_get_nodes):
        with patch.multiple(
            "sheepdog.download",
            key_for=fake_key_for,
            key_for_node=fake_key_for_node,
            urls_from_index_client=fake_urls_from_index_client,
        ):
            if os.environ.get("GDC_FAKE_AUTH"):
                run_with_fake_auth()
            else:
                run_for_development(debug=debug, threaded=True)


if __name__ == "__main__":
    debug = bool(os.environ.get("SHEEPDOG_DEBUG", True))
    if os.environ.get("GDC_FAKE_DOWNLOAD") == "True":
        run_with_fake_download()
    else:
        if os.environ.get("GDC_FAKE_AUTH") == "True":
            run_with_fake_auth()
        else:
            run_with_fake_authz()
