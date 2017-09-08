"""
"""

import collections
import json

import flask
import flask_sqlalchemy_session
import userdatamodel
from userdatamodel.user import AccessPrivilege, HMACKeyPair, User

from sheepdog import models
from sheepdog.errors import (
    AuthError,
    InternalError,
    InvalidTokenError,
)


class FederatedUser(object):

    def __init__(self, hmac_keypair=None, user = None):
        self._phsids = {}
        if hmac_keypair is None:
            self.hmac_keypair = None
            self.user = user
            self.username = user.username
            self.id = user.id
        else:
            self.hmac_keypair = hmac_keypair
            self.user = hmac_keypair.user
            self.username = hmac_keypair.user.username
            self.id = hmac_keypair.user.id
        self.project_ids = {}
        self._roles = collections.defaultdict(set)
        self.role = None
        self.mapping = {}

    def get_projects_mapping(self, phsid):
        if phsid in self.mapping:
            return self.mapping[phsid]
        with flask.current_app.db.session_scope():
            project = (
                flask.current_app
                .db
                .nodes(models.Project)
                .props(dbgap_accession_number=phsid)
                .first()
            )
            self.mapping[phsid] = []
            if project:
                self.mapping[phsid] = [
                    project.programs[0].name + '-' + project.code
                ]
            else:
                program = (
                    flask.current_app
                    .db
                    .nodes(models.Program)
                    .props(dbgap_accession_number=phsid)
                    .first()
                )
                if program:
                    self.mapping[phsid] = [
                        program.name + '-' + node.code
                        for node in program.projects
                    ]
        return self.mapping[phsid]

    def __str__(self):
        str_out = {
            'id': self.user.id,
            'access_key': (
                self.hmac_keypair.access_key if self.hmac_keypair else None
            ),
            'username': self.user.username,
            'is_admin': self.user.is_admin
        }
        return json.dumps(str_out)

    def logged_in(self):
        if not self.user.username:
            raise InvalidTokenError()

    @property
    def roles(self):
        if not self._roles:
            self.set_roles()
        return self._roles

    @property
    def phsids(self):
        if not self._phsids:
            self.set_phs_ids()
        return self._phsids

    def set_roles(self):
        for phsid, roles in self.phsids.iteritems():
            for project in self.get_projects_mapping(phsid):
                for role in roles:
                    self._roles[project].add(role)

    def set_phs_ids(self):
        self._phsids = flask.current_app.auth.get_user_projects(self.user)
        return self._phsids

    def get_role_by_dbgap(self, dbgap_no):
        project = (
            flask_sqlalchemy_session.current_session
            .query(userdatamodel.user.Project)
            .filter(userdatamodel.user.Project.auth_id == dbgap_no)
            .first()
        )
        if not project:
            raise InternalError("Don't have project with {0}".format(dbgap_no))
        roles = (
            flask_sqlalchemy_session.current_session
            .query(AccessPrivilege)
            .filter(AccessPrivilege.user_id == flask.g.user.id)
            .filter(AccessPrivilege.project_id == project.id)
            .first()
        )
        if not roles:
            raise AuthError("You don't have access to the data")
        return roles

    def fetch_project_ids(self, role='_member_'):
        result = []
        for phsid, roles in self.phsids.iteritems():
            if role in roles:
                result += self.get_projects_mapping(phsid)
        return result

    def get_project_ids(self, role='_member_'):
        self.logged_in()
        if role not in self.project_ids:
            self.project_ids[role] = self.fetch_project_ids(role)
        return self.project_ids[role]
