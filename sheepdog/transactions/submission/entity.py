# pylint: disable=protected-access

from sheepdog import models
from sheepdog.utils import (
    set_indexd_state,
    get_indexd_state,
)
from sheepdog.globals import (
    ENTITY_STATE_TRANSITIONS,
    FILE_STATE_TRANSITIONS,
    FILE_STATE_KEY,
    STATE_KEY,
    SUBMITTABLE_FILE_STATES,
    SUBMITTABLE_STATES,
)
from sheepdog.transactions.entity_base import EntityBase, EntityErrors


class SubmissionEntity(EntityBase):

    """Models an entity to be marked submitted."""

    def __init__(self, transaction, node):
        super(SubmissionEntity, self).__init__(transaction, node)
        self.action = 'submit'

    def version_node(self):
        """
        Clone the current state of ``entity.node`` to the ``versioned_nodes``
        table in the database.
        """
        self.logger.info('Versioning {}.'.format(self.node))
        with self.transaction.db_driver.session_scope() as session:
            session.add(models.VersionedNode.clone(self.node))

    @property
    def secondary_keys(self):
        """Return the list of unique dicts for the node."""
        return self.node._secondary_keys

    @property
    def secondary_keys_dicts(self):
        """Return the list of unique tuples for the node."""
        return self.node._secondary_keys_dicts

    @property
    def pg_secondary_keys(self):
        """Return the list of unique tuples for the node type"""

        return getattr(self.node, '__pg_secondary_keys', [])

    def submit(self):
        """
        Check whether this is a valid transition and transition the entity's
        state to `submitted` (and file_state if valid).
        """
        self.logger.info('Submitting {}.'.format(self.node))
        to_state = 'submitted'
        current_state = self.node._props.get(STATE_KEY, None)
        current_file_state = get_indexd_state(self.node.node_id)

        # Check node.state
        if current_state not in SUBMITTABLE_STATES:
            return self.record_error(
                "Unable to submit node with state: '{}'".format(current_state),
                type=EntityErrors.INVALID_PROPERTY
            )

        # Conditionally update file_state
        if current_file_state in SUBMITTABLE_FILE_STATES:
            set_indexd_state(self.node.node_id, to_state)

        self.node.props[STATE_KEY] = to_state

        # Clone to version table
        self.version_node()
