import flask
import flask_sqlalchemy_session
import sqlalchemy
import userdatamodel
from userdatamodel.user import AccessPrivilege

from sheepdog import models
from sheepdog.errors import (
    AuthError,
    InternalError,
    NotFoundError,
)
from sheepdog.globals import (
    ROLES,
    MEMBER_DOWNLOADABLE_STATES,
    SUBMITTER_DOWNLOADABLE_STATES,
)
from sheepdog.auth.federated_user import FederatedUser


class AuthDriver(object):
    """
    Responsible for checking user's access permission and getting user
    information from the token passed to gdcapi.
    """

    def __init__(self, auth_conf, internal_auth):
        return

    def get_user_projects(self, user):
        if not user:
            raise AuthError('Please authenticate as a user')
        if not flask.g.user:
            flask.g.user = FederatedUser(user)
        results = (
            flask_sqlalchemy_session.current_session
            .query(
                userdatamodel.user.Project.auth_id, AccessPrivilege
            )
            .join(AccessPrivilege.project)
            .filter(AccessPrivilege.user_id == flask.g.user.id)
            .all()
        )
        return_res = {}
        if not results:
            raise AuthError('No project access')
        for item in results:
            dbgap_no, user_access = item
            return_res[dbgap_no] = user_access.privilege
        return return_res

    def check_nodes(self, nodes):
        """
        Check if user have access to all of the dids, return 403 if user
        doesn't have access on any of the file, 404 if any of the file
        is not in psqlgraph, 200 if user can access all dids
        """
        for node in nodes:
            node_acl = node.acl
            if node_acl == ['open']:
                continue
            elif node_acl == []:
                raise AuthError(
                    'Requested file %s does not allow read access' %
                    node.node_id, code=403)
            else:
                if flask.g.user.token is None:
                    raise AuthError('Please specify a X-Auth-Token')
                else:
                    user_acl = (
                        flask.g.user.get_phs_ids(self.get_role(node)))
                    if not(set(node_acl) & set(user_acl)):
                        raise AuthError(
                            "You don't have access to the data")
        return 200

    def get_role(self, node):
        state = node.state
        file_state = node.file_state
        # if state is live, it's a processed legacy file
        if state == 'live':
            return ROLES['GENERAL']
        elif node.project_id:
            with flask.current_app.db.session_scope():
                program, project = node.project_id.split('-', 1)
                try:
                    project = (
                        flask.current_app.db
                        .nodes(models.Project)
                        .props(code=project)
                        .path('programs')
                        .props(name=program)
                        .one()
                    )
                except sqlalchemy.orm.exc.MultipleResultsFound:
                    raise InternalError(
                        "Multiple results found for file {}'s project {}"
                        .format(node.node_id, node.project_id)
                    )
                except sqlalchemy.orm.exc.NoResultFound:
                    raise InternalError(
                        "No results found for file {}'s project {}"
                        .format(node.node_id, node.project_id)
                    )

                # for general users with '_member_' role, allow
                # download if project is released and file is submitted
                # and file_state is at or after "submitted"
                allow_general_access = (
                    project.released is True and
                    state == 'submitted' and
                    file_state in MEMBER_DOWNLOADABLE_STATES)

                # for submitters with "download" role, allow download
                # if file_state is at or after "uploaded"
                allow_submitter_access = file_state in SUBMITTER_DOWNLOADABLE_STATES

                if allow_general_access:
                    return ROLES['GENERAL']

                elif allow_submitter_access:
                    return ROLES['DOWNLOAD']

                else:
                    raise NotFoundError(
                        "Data with id {} not found"
                        .format(node.node_id))

        else:
            # node does not have project_id and is not live
            raise NotFoundError("Data with id {} not found".format(node.node_id))

    def filter_nodes(self, nodes):
        """
        Filters nodes that are not authorized for the user.
        """
        for node in nodes:
            if node.acl == ['open']:
                yield node
            else:
                try:
                    user_acl = set(
                        flask.g.user.get_phs_ids(self.get_role(node))
                    )
                    if set(node.acl) & user_acl:
                        yield node
                except:
                    pass

    def has_protected(self, nodes):
        """
        Checks if any of the nodes are protected.
        """
        return any([node.acl != ['open'] for node in nodes])
