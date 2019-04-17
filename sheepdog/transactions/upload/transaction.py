"""
Define the ``UploadTransaction`` class.
"""

import re
from collections import Counter

# Validating Entity Existence in dbGaP
from datamodelutils import validators
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.attributes import flag_modified

from sheepdog.auth import dbgap
from sheepdog import models
from sheepdog import utils
from sheepdog.errors import UserError, HandledIntegrityError
from sheepdog.globals import (
    case_cache_enabled,
    TX_LOG_STATE_ERRORED,
    TX_LOG_STATE_FAILED,
    TX_LOG_STATE_SUCCEEDED,
)
from sheepdog.transactions.upload.entity import EntityErrors
from sheepdog.transactions.transaction_base import TransactionBase
from sheepdog.transactions.upload.entity_factory import UploadEntityFactory


KEYS_REGEXP = re.compile(r"_props ->> '([^']+)+'::text")
VALUES_REGEXP = re.compile(r"=\(([^\(\)]+)\)")


class UploadTransaction(TransactionBase):
    """
    An UploadTransaction should be used as a context manager. This way, we can
    catch ingress and egress to handle commits and rollbacks properly.

    In order to make sure that the database session used in the upload
    transaction is isolated, nest it in a session scope that can't inherit a
    session from a parent context:

    .. code-block:: python

        with flask.current_app.db.session_scope(can_inherit=False):
            with UploadTransaction(**kwargs) as transaction:
                # ... do work ...
    """

    REQUIRED_PROJECT_STATES = ["open"]

    def __init__(self, **kwargs):
        """
        Initizalize the UploadTransaction.

        Keyword Args:
            See TransactionBase.__init__()
        """
        #: HTTP[S] proxies used for requests to external services
        # Base class doesn't know about this, so pop first
        self.external_proxies = kwargs.pop("external_proxies", {})
        super(UploadTransaction, self).__init__(**kwargs)
        self.documents = []
        self.json_validator = validators.GDCJSONValidator()

        # The dbGapXReferencer conditionally requires cases to exist in
        # dbGaP prior to submission to the GDC
        self.dbgap_x_referencer = dbgap.dbGaPXReferencer(
            self.db_driver, self.logger, proxies=self.external_proxies
        )

        self._config = kwargs["flask_config"]

    def get_phsids(self):
        """Fetch the phsids for the current project."""
        project = utils.lookup_project(self.db_driver, self.program, self.project)
        program = project.programs[0]
        numbers = [project.dbgap_accession_number, program.dbgap_accession_number]
        return [n for n in numbers if n is not None]

    def parse_doc(self, name, doc_format, doc, data):
        """Add/parse a document to the transaction."""
        self.parse_entities(data)
        tx_document = models.submission.TransactionDocument(
            name=name, doc_format=doc_format, doc=doc
        )
        with self.fetch_transaction_log() as tx_log:
            tx_log.documents.append(tx_document)

    def parse_entities(self, docs):
        """
        Take a list of `docs` (json representations of nodes) and add each as a
        TransactionEntity to the transaction.

        Args:
            doc (list): list of json Node representations

        Return:
            None
        """
        if isinstance(docs, dict):
            docs = [docs]
        for doc in docs:
            self.add_entity(doc)

        with self.fetch_transaction_log() as tx_log:
            tx_log.canonical_json += docs
            # Mark the column dirty, or else sqlalchemy won't know it was
            # mutated.
            flag_modified(tx_log, "canonical_json")

        self.json_validator.record_errors(self.entities)
        self.instantiate()
        self.pre_validate()

    def fetch_transaction_log_documents(self):
        """Returns a list of documents from a bulk transaction"""

        with self.fetch_transaction_log() as tx_log:
            return tx_log.documents

    def specify_errors(self):
        """
        Parse the error (type, message, etc) for errors may not have been
        classified with an ERROR_TYPE.
        """
        return [entity.specify_errors() for entity in self.entities]

    def pre_validate(self):
        """
        Cover validation steps that are not JSON Schema or graph validation.
        """
        # Make sure that all entities are unique by checking for duplicate
        # secondary keys.
        checked = set()
        entities_secondary_keys = (
            entity.secondary_keys
            for entity in self.valid_entities
            if entity.secondary_keys
        )
        for secondary_keys in entities_secondary_keys:
            if secondary_keys in checked:
                self.record_error(
                    "Entity is not unique, {}".format(secondary_keys),
                    type=EntityErrors.NOT_UNIQUE,
                )
            checked.add(secondary_keys)

        self.specify_errors()

    def post_validate(self):
        """
        Handle graph linking and validation. Should be called after
        ``self.pre_validate()``
        """
        self.create_links()
        self.graph_validator.record_errors(self.db_driver, self.valid_entities)
        self.specify_errors()

    def instantiate(self):
        """Create a SQLAlchemy model for all transaction entities."""
        for entity in self.valid_entities:
            entity.instantiate()

    def create_links(self):
        """Construct edges between all transaction entities."""
        for entity in self.valid_entities:
            entity.set_association_proxies()

    def flush(self):
        """
        Flush entities to the session.

        This is helpful to allow querying of the current state of the graph and
        cause flush hooks to be called.

        Note: post_validate was once done at the end of this function. This
        caused validation to be prematurely executed for bulk transactions.
        Now, post_validation is left to the caller.
        """
        for entity in self.valid_entities:
            entity.flush_to_session()
        try:
            self.session.flush()
        except IntegrityError as e:
            # don't handle non-unique constraint errors
            if "duplicate key value violates unique constraint" not in e.message:
                raise
            values = VALUES_REGEXP.findall(e.message)
            if not values:
                raise
            values = [v.strip() for v in values[0].split(",")]
            keys = KEYS_REGEXP.findall(e.message)
            if len(keys) == len(values):
                values = dict(zip(keys, values))
                entities = []
                label = None
                for en in self.valid_entities:
                    for k, v in values.items():
                        if getattr(en.node, k, None) != v:
                            break
                    else:
                        if label and label != en.node.label:
                            break
                        entities.append(en)
                        label = en.node.label
                else:  # pylint: disable=useless-else-on-loop
                    # https://github.com/PyCQA/pylint/pull/2760
                    for entity in entities:
                        entity.record_error(
                            "{} with {} already exists in the GDC".format(
                                entity.node.label, values
                            ),
                            keys=keys,
                        )
                    if entities:
                        raise HandledIntegrityError()
            self.record_error("{} already exists in the GDC".format(values))
            raise HandledIntegrityError()

    @property
    def status_code(self):
        """
        Return the current HTTP status code at any point during a transaction.
        """
        role_is_create = self.role == "create"
        if self.dry_run or (self.success and not role_is_create):
            return 200
        elif self.success and role_is_create:
            return 201
        else:
            return 400

    @property
    def json(self):
        """
        Return a JSON representation of transaction status, errors, entities,
        etc.
        """
        doc = dict(
            self.base_json,
            **{
                "created_entity_count": self.created_entity_count,
                "updated_entity_count": self.updated_entity_count,
            }
        )
        if case_cache_enabled():
            doc["cases_related_to_updated_entities_count"] = len(
                {
                    case["id"]
                    for entity in doc["entities"]
                    for case in entity["related_cases"]
                    if entity["action"] == "update"
                }
            )
            doc["cases_related_to_created_entities_count"] = len(
                {
                    case["id"]
                    for entity in doc["entities"]
                    for case in entity["related_cases"]
                    if entity["action"] == "create"
                }
            )
        return doc

    @property
    def message(self):
        """
        Return a message describing the transaction at any point in time.
        """
        if not self.success:
            message = "Transaction aborted due to "
            if self.entity_error_count:
                count = self.entity_error_count
                message += "{} invalid {}".format(
                    count, "entities" if count > 1 else "entity"
                )
            if self.entity_error_count and self.transactional_errors:
                message += " and "
            if self.transactional_errors:
                message += "{} transactional error(s)".format(
                    self.transactional_error_count
                )
        else:
            if self.dry_run:
                return (
                    "Transaction would have been successful. User selected dry"
                    " run option, transaction aborted, no data written to"
                    " database."
                )
            else:
                message = "Transaction successful"
        return message + "."

    @property
    def created_entity_count(self):
        """Return the count of created entities."""
        if not self.success:
            return 0
        return len([e for e in self.entities if e.action == "create"])

    @property
    def updated_entity_count(self):
        """Return the count of updated entities."""
        if not self.success:
            return 0
        return len([e for e in self.entities if e.action == "update"])

    def add_entity(self, doc):
        """
        Add an entity to the transaction.

        Args:
            doc (dict):
                A dictionary (JSON) containing the data from which to generate
                the entity

        Return:
            None
        """
        try:
            entity = UploadEntityFactory.create(self, doc, self._config)
            entity.parse(doc)
            self.entities.append(entity)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.exception(e)
            self.record_error("Unable to parse entity")


class BulkUploadTransaction(TransactionBase):

    REQUIRED_PROJECT_STATES = ["open"]

    def __init__(self, **kwargs):
        """
        BulkUploadTransaction inherits from TransactionBase to support the
        processing of multiple documents in a single database session.

        Keyword Args:
            TransactionBase.__init__()
        """
        self.external_proxies = kwargs.pop("external_proxies", {})
        super(BulkUploadTransaction, self).__init__(**kwargs)
        self.flush_timestamp = None
        self.transactional_errors = []
        self.subtransactions = []

    @property
    def success(self):
        return (
            not self.transactional_errors
            # All subtransactions are successful
            and all([t.success for t in self.subtransactions])
            # All subtransactions are non-empty
            and all([t.entities for t in self.subtransactions])
        )

    @property
    def status_code(self):
        """
        Returns the current HTTP status code at any point during a transaction.
        """
        if self.dry_run:
            return 200
        elif not self.success:
            return 400
        elif self.role == "create":
            return 201
        else:
            return 200

    def add_doc(self, name, doc_format, doc, data):
        """
        TODO
        """
        sub_transaction = UploadTransaction(
            program=self.program,
            project=self.project,
            role=self.role,
            dry_run=self.dry_run,
            db_driver=self.db_driver,
            user=self.user,
            document_name=name,
            logger=self.logger,
            transaction_id=self.transaction_id,
            signpost=self.signpost,
            flask_config=self.config,
            external_proxies=self.external_proxies,
        )
        sub_transaction.parse_doc(name, doc_format, doc, data)
        self.subtransactions.append(sub_transaction)

    def flush(self):
        """
        Make sure all necessary entites have their node bound to the session
        and attempt a session flush.
        """
        for subtransaction in self.subtransactions:
            subtransaction.flush()
        self.session.flush()

    def post_validate(self):
        """
        TODO
        """
        self.check_for_duplicates()
        for sub_tx in self.subtransactions:
            sub_tx.post_validate()
        self.record_errors_for_empty_subtransactions()
        self.record_errors_for_empty_transaction()

    def record_errors_for_empty_subtransactions(self):
        """Record transactional_errors for empty subtransactions."""
        for subtrans in self.subtransactions:
            subtrans.record_errors_for_empty_transaction()

    def commit(self, _=True):
        """
        If successful, write the result of this transaction to the database,
        otherwise rollback.
        """
        self.write_transaction_log()

        if not self.success:
            self.set_transaction_log_state(TX_LOG_STATE_FAILED)
            self.rollback()
            raise UserError(message="Bulk Transaction failed", json=self.json)

        if self.dry_run:
            self.set_transaction_log_state(TX_LOG_STATE_SUCCEEDED)
            return self.rollback()

        try:
            self.flush()
            self.session.commit()
            self.set_transaction_log_state(TX_LOG_STATE_SUCCEEDED)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.exception(e)
            msg = "Unable to write to database, please try again"
            self.transactional_errors.append(msg)
            self.set_transaction_log_state(TX_LOG_STATE_ERRORED)
            self.session.rollback()

    def check_for_duplicates(self):
        """
        Check and record errors for duplicated entities.
        """
        if not self.success:
            return

        def duplicates(counter):
            """Filter for items in a counter with count > 1."""
            return {item: count for item, count in counter if count > 1}

        ids = [entity.entity_id for entity in self.entities]
        dup_ids = duplicates(Counter(ids).items())
        secondary_keys = [e.secondary_keys for e in self.entities]
        dup_secondary_keys = duplicates(Counter(secondary_keys).iteritems())
        # Check secondary_keys
        for sk in dup_secondary_keys.keys():
            for entity in self.entities:
                if entity.secondary_keys == sk:
                    entity.record_error(
                        "Entity is duplicated elsewhere in bulk transaction",
                        type=EntityErrors.NOT_UNIQUE,
                    )

        # Check GDC ids
        for ID in dup_ids.keys():
            for entity in self.entities:
                if entity.entity_id == ID:
                    entity.record_error(
                        "Entity is duplicated elsewhere in bulk transaction",
                        type=EntityErrors.NOT_UNIQUE,
                    )

    def set_subtransaction_document_response_json(self):
        """
        Set the document response json.

        The document information that corresponds to each subtransaction is
        initially stored with ``response_json`` left null. This function should
        be called before :func:`write_transaction_log` and after :func:`flush`.
        """
        with self.fetch_transaction_log() as sub_tx_log:
            for doc, sub_tx in zip(sub_tx_log.documents, self.subtransactions):
                doc.response_json = sub_tx.json

    def write_transaction_log(self):
        with self.fetch_transaction_log() as tx_log:
            tx_log.submitter = self.user.username
            if self.success:
                for entity in self.entities:
                    snapshot = models.submission.TransactionSnapshot()
                    snapshot.entity_id = entity.node.node_id
                    snapshot.old_props = entity.old_props
                    snapshot.new_props = entity.node.props
                    snapshot.action = entity.action
                    tx_log.entities.append(snapshot)

            self.set_subtransaction_document_response_json()
            tx_log.timestamp = self.flush_timestamp

    @property
    def entities(self):
        """
        Return all entities from all subtransactions.
        """
        return [e for t in self.subtransactions for e in t.entities]

    @property
    def subtransaction_json(self):
        """
        Return the json for all subtransactions.
        """
        return [t.json for t in self.subtransactions]

    @property
    def message(self):
        """
        Return a message describing the state of the bulk transaction (success
        or failure).
        """
        if self.success:
            return "Bulk Transaction succeeded."
        else:
            return "Bulk Transaction failed."

    @property
    def json(self):
        """
        TODO
        """
        subtransaction_json = self.subtransaction_json
        entity_error_count = sum([j["entity_error_count"] for j in subtransaction_json])
        updated_entity_count = sum(
            [j["updated_entity_count"] for j in subtransaction_json]
        )
        created_entity_count = sum(
            [j["created_entity_count"] for j in subtransaction_json]
        )
        document_error_count = len([t for t in self.subtransactions if not t.success])

        return {
            "transaction_id": self.transaction_id,
            "transactional_errors": self.transactional_errors,
            "success": self.success,
            "message": self.message,
            "entity_error_count": entity_error_count,
            "updated_entity_count": updated_entity_count,
            "created_entity_count": created_entity_count,
            "document_error_count": document_error_count,
            "code": self.status_code,
            "subtransactions": [
                {"name": t.document_name, "response_json": t.json}
                for t in self.subtransactions
            ],
        }
