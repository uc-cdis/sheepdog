# pylint: disable=protected-access

from sheepdog import models
from sheepdog.globals import (
    ENTITY_STATE_TRANSITIONS,
    FILE_STATE_TRANSITIONS,
    FILE_STATE_KEY,
    STATE_KEY,
    REQUEST_SUBMIT_KEY,
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

    def user_request_submit(self):
        """
        Check whether node is in a valid state
        and change request_submit property to True
        """
        self.logger.info('User Submitting {}.'.format(self.node))

        current_state = self.node._props.get(STATE_KEY, None)

        # Check if node in submittable state
        if current_state not in SUBMITTABLE_STATES:
            return self.record_error(
                "Unable to submit node with state: '{}'".format(current_state),
                type=EntityErrors.INVALID_PROPERTY
            )

        # Set node to be requested for submission
        # TODO: THIS MUST BE DONE ON PROJECT LEVEL
        self.node.props[REQUEST_SUBMIT_KEY] = True
