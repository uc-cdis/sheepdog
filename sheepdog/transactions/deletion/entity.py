from sheepdog.globals import (
    submitted_state,
    ALLOWED_DELETION_STATES,
)
from sheepdog.transactions.entity_base import EntityBase, EntityErrors
from sheepdog.transactions.transaction_base import MissingNode


ALLOWED_DELETION_FILE_STATES = [submitted_state(), None]


class DeletionEntity(EntityBase):

    def __init__(self, transaction, node):
        super(DeletionEntity, self).__init__(transaction, node)
        self.action = 'delete'
        self.dependents = {
            # entity.node.node_id: entity
        }

        if isinstance(node, MissingNode):
            self.neighbors = []
            return

        self.neighbors = [edge.src for edge in node.edges_in]

        # Check user permissions for deleting nodes
        roles = self.transaction.user.roles.get(self.transaction.project_id, [])
        if 'delete' not in roles:
            self.record_error(
                'You do not have delete permission for project {}'
                .format(self.transaction.project_id)
            )

    @property
    def related_cases(self):
        """Override super.related_cases because by the time we go to access
        it, the node has been deleted in the session

        By the time that the related cases were queried from the
        database, the relationships had already been deleted, this
        relationship is stored for later use,

        """

        return self.transaction.related_cases.get(self.node.node_id, [])

    @property
    def secondary_keys(self):
        """Return the list of unique dicts for the node"""

        if isinstance(self.node, MissingNode):
            return []
        else:
            return self.node._secondary_keys

    @property
    def secondary_keys_dicts(self):
        """Return the list of unique tuples for the node"""

        if isinstance(self.node, MissingNode):
            return []
        else:
            return self.node._secondary_keys_dicts

    @property
    def pg_secondary_keys(self):
        """Return the list of unique tuples for the node type"""

        if isinstance(self.node, MissingNode):
            return []
        else:
            return getattr(self.node, '__pg_secondary_keys', [])

    def _delete(self):
        """Delete the node in the current session.

        Also, manually clear out the association proxies for the node
        to prevent issues in the session-flush hooks that try and get
        the history of the node.

        """

        if not self.node_exists:
            return

        for edge in self.node.get_edges():
            self.transaction.session.delete(edge)

        for association in self.node._pg_edges:
            setattr(self.node, association, [])

        self.transaction.session.delete(self.node)

    def recursive_test_deletion(self):
        self.logger.info('Attempting deletion of {}'.format(self))

        subentities = [
            DeletionEntity(self.transaction, n)
            for n in self.neighbors if n
        ]

        self.transaction.graph_validator.record_errors(
            self.transaction.db_driver, subentities)

        invalid_neighbor_entities = [e for e in subentities if not e.is_valid]

        for entity in invalid_neighbor_entities:
            if entity.node_exists:
                self.dependents[entity.node.node_id] = entity
                entity._delete()
                entity.recursive_test_deletion()

                # Add child dependencies to this entity
                self.dependents.update(entity.dependents)

    def error_for_dependents(self):
        """Record error if it has dependents"""

        self.recursive_test_deletion()

        if self.dependents:
            self.record_error(
                ("Unable to delete entity because {} "
                 "others directly or indirectly depend on it. "
                 "You can only delete this entity by deleting its dependents "
                 "prior to, or during the same transaction as this one.")
                .format(len(self.dependents)),
                keys=[],
                type=EntityErrors.INVALID_LINK,
                dependents=[{
                    'id': node_id,
                    'type': entity.node.label,
                } for node_id, entity in self.dependents.iteritems()],
            )

    def error_for_state(self):
        """Record an if the entity is in an invalid state"""

        state = self.node._props.get('state')

        if state not in ALLOWED_DELETION_STATES:
            if state == 'submitted':
                message = ("This node has been submitted. "
                           "Deletion is disallowed for submitted entities. "
                           "This node must be redacted.")

            else:
                message = ("Unable to delete entity because it is "
                           "in state '{}'.".format(self.node.state))

            self.record_error(
                message,
                keys=['state'],
                type=EntityErrors.INVALID_PERMISSIONS)

    def error_for_file_state(self):
        """Record an if the entity is in an invalid state"""

        if 'file_state' not in self.node.__pg_properties__:
            return

        file_state = self.node._props.get('file_state')

        if file_state not in ALLOWED_DELETION_FILE_STATES:
            message = ("This node has file_state '{file_state}'. "
                       "Deletion is disallowed for entities that have "
                       "raw data uploaded to the GDC.  In order to delete "
                       "this node you must first delete the raw data with "
                       "the Data Transfer Tool."
                       .format(file_state=self.node.file_state))

            self.record_error(
                message,
                keys=['file_state'],
                type=EntityErrors.INVALID_PERMISSIONS)


    def test_deletion(self):
        """Attempt to delete this node and record errors for policy
        violations

        """

        self.logger.info('Testing deletion tree from {}'.format(self))

        self.error_for_state()
        self.error_for_file_state()
        self.error_for_dependents()
