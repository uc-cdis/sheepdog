from flask import current_app
from sheepdog import dictionary
from sheepdog.errors import UserError
from sheepdog.globals import (
    case_cache_enabled,
    TX_LOG_STATE_FAILED,
)
from sheepdog.transactions.entity_base import EntityErrors
from sheepdog.transactions.deletion.entity import DeletionEntity
from sheepdog.transactions.transaction_base import MissingNode, TransactionBase


class DeletionTransaction(TransactionBase):

    REQUIRED_PROJECT_STATES = ['open']

    def __init__(self, **kwargs):
        super(DeletionTransaction, self).__init__(role='delete', **kwargs)
        self.fields_to_delete = kwargs.get('fields', None)
        self.to_delete = kwargs.get('to_delete', None)

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

        return 'Deletion transaction failed.'

    @property
    def deleted_entity_count(self):
        """Returns the **total** error count: the sum of transactional and
        entity error counts.

        """
        if not self.success:
            return 0
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
            if self.to_delete is not None:
                # set to_delete in sysan to True or False if present
                for e in self.valid_entities:
                    e.node.sysan['to_delete'] = self.to_delete
            else:
                map(self.session.delete, [e.node for e in self.valid_entities])

    def _delete_fields(self):
        """
        Sets each field value to None for each field in self.fields_to_delete
        """
        if not self.fields_to_delete:
            raise Exception(
                'Something went terribly wrong,'
                ' DeletionTransaction()._delete_fields is expected'
                ' to be called only when fields_to_delete are set'
            )

        if self.success:
            for e in self.valid_entities:
                for field in self.fields_to_delete.split(','):
                    if field in e.node.props:
                        field_is_protected = any(
                            [field in dictionary.schema[e.node.label].get(protected_category, [])
                             for protected_category in ['required', 'systemProperties']]
                        )
                        # Below is a sad workaround for inconsistent gdcdictionary :|
                        # Will be safe to remove once 'required' fields is a
                        # complete and tested set for all node types in gdcdictionary.schemas
                        if current_app.config.get('IS_GDC', True):
                            if field in ['submitter_id', 'project_id', 'state', 'file_state']:
                                field_is_protected = True

                        if field_is_protected:
                            raise UserError(
                                'Unable to delete protected field "{}" in a {} node'
                                .format(field, e.node.label)
                            )
                        else:
                            e.node.props[field] = None
                    else:
                        raise UserError(
                            'Attempted to delete non-existing field "{}" in a node {}'
                            .format(field, e.node)
                        )

    def test_deletion(self, delete_function):
        """Delete nodes or fields from session, and always rollback"""

        try:
            delete_function()

            if not self.fields_to_delete:
                for entity in self.valid_entities:
                    entity.test_deletion()

        finally:
            # ==== THIS LINE IS VERY IMPORTANT ====
            # The deletion is tested by ACTUALLY DELETING THE NODES in the
            # local session.  This rolls this back.
            self.session.rollback()

    def delete(self, ids):
        """Delete nodes or fields from session and commit if successful"""

        if self.fields_to_delete:
            delete_function = self._delete_fields
        else:
            delete_function = self._delete_entities

        self.get_nodes(ids)
        self.test_deletion(delete_function)

        if self.success:
            delete_function()
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
