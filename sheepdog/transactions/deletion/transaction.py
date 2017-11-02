import flask
from gdcapi.errors import UserError

from sheepdog import utils
from sheepdog.globals import (
    case_cache_enabled,
    FLAG_IS_ASYNC,
    TX_LOG_STATE_FAILED,
)
from sheepdog.transactions.entity_base import EntityErrors
from sheepdog.transactions.deletion.entity import DeletionEntity
from sheepdog.transactions.transaction_base import MissingNode, TransactionBase


class DeletionTransaction(TransactionBase):

    REQUIRED_PROJECT_STATES = ['open']

    def __init__(self, **kwargs):
        super(DeletionTransaction, self).__init__(role='delete', **kwargs)

        # see DeletionEntity.related_cases() docstring for details
        self.related_cases = {
            # node_id: [{"id":.., "submitter_id": ..}]
        }

    def write_transaction_log(self):
        """Save a log noting this project was opened"""

        with self.fetch_transaction_log() as tx_log:
            tx_log.documents = [self.new_stub_transaction_document()]

        super(DeletionTransaction, self).write_transaction_log()

    @property
    def message(self):
        if self.success and not self.dry_run:
            return 'Successfully deleted {} entities'.format(
                len(self.entities))
        elif self.success and self.dry_run:
            return 'Dry run successful. Would have deleted {} entities'.format(
                len(self.entities))
        else:
            return 'Deletion transaction failed.'

    @property
    def deleted_entity_count(self):
        """Returns the **total** error count: the sum of transactional and
        entity error counts.

        """
        if not self.success:
            return 0
        else:
            return len(self.entities)

    @property
    def dependent_ids(self):
        return ','.join([
            dependent_id for entity in self.entities
            for dependent_id in entity.dependents
        ])

    @property
    def json(self):
        return dict(self.base_json, **{
            'deleted_entity_count': self.deleted_entity_count,
            'dependent_ids': self.dependent_ids,
        })

    def _delete_entities(self):
        if self.success:
            map(self.session.delete, [e.node for e in self.valid_entities])

    def test_deletion(self):
        """Delete nodes from session, and always rollback"""

        try:
            self._delete_entities()

            for entity in self.valid_entities:
                entity.test_deletion()

        finally:
            # ==== THIS LINE IS VERY IMPORTANT ====
            # The deletion is tested by ACTUALLY DELETING THE NODES in the
            # local session.  This rolls this back.
            self.session.rollback()

    def delete(self, ids):
        """Delete nodes from session and commit if successful"""

        self.get_nodes(ids)
        self.test_deletion()

        if self.success:
            self._delete_entities()
            self.commit()
        else:
            self.session.rollback()
            self.set_transaction_log_state(TX_LOG_STATE_FAILED)
            self.write_transaction_log()

    def get_nodes(self, ids):
        """Populates self.entities with nodes from :param:`ids`"""

        nodes = (self.db_driver.nodes()
                 .props(project_id=self.project_id)
                 .ids(ids)
                 .all())

        self.entities = [
            DeletionEntity(self, node)
            for node in nodes
        ]

        # Look for missing entitites
        entity_ids = {e.node.node_id for e in self.entities}
        missing_ids = [ID for ID in ids if ID not in entity_ids]

        for ID in missing_ids:
            missing = DeletionEntity(self, MissingNode())
            missing.record_error(
                'Entity not found.',
                keys=['id'],
                id=ID,
                type=EntityErrors.NOT_FOUND)
            self.entities.append(missing)

        # see DeletionEntity.related_cases() docstring for details
        if case_cache_enabled():
            self.related_cases = {
                node.node_id: [{
                    'id': c.node_id,
                    'submitter_id': c.submitter_id
                } for c in node._related_cases_from_cache]
                for node in nodes
            }
