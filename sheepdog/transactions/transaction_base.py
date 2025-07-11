"""
Define the ``TransactionBase`` class, which ``UploadTransaction`` inherits
from.
"""

from contextlib import contextmanager

import flask
from flask import current_app
from datamodelutils import validators

from sheepdog import auth
from sheepdog import models
from sheepdog import utils
from sheepdog.errors import UserError
from sheepdog.globals import (
    MESSAGE_500,
    TX_LOG_STATE_ERRORED,
    TX_LOG_STATE_FAILED,
    TX_LOG_STATE_PENDING,
    TX_LOG_STATE_SUCCEEDED,
)


class MissingNode(object):
    """Placeholder class to stub properties of a missing node."""

    def __init__(self, ID=None):
        self.node_id = ID
        self.label = None
        self.props = {}


class TransactionBase(object):
    """
    Parent class for sheepdog API transactions.

    * Children must define:

        REQUIRED_PROJECT_STATES = [..]

    * Children should always call ``super(..., self).__init__()``
    """

    REQUIRED_PROJECT_STATES = []

    def __init__(self, program, project, **kwargs):
        """
        Collected functionality for submission transactions.

        Args:
            program: Program.name of the transaction
            project: Project.code of the transaction
            document_name: Name of a file containing mutation
            dry_run: If True, the transaction will not be committed
            role: The role required to take this transaction
            user: The initializer of the transaction
            index_client: index client (default: capp.index_client)
            db_driver: PsqlGraph driver (default: capp.db)
            logger: Logging driver (default: capp.logger)
            transaction_id: Optionally inherit and write to an existing
                TransactionLog. If this is not provided, a new TransactionLog
                will be created.
        """
        self.program = program
        self.project = project
        # Optional
        self.document_name = kwargs.pop("document_name", None)
        self.dry_run = kwargs.pop("dry_run", False)
        self.role = kwargs.pop("role", None)
        # To be pulled from flask request context if not provided
        self.logger = kwargs.pop("logger", None) or current_app.logger
        self.index_client = kwargs.pop("index_client", None) or current_app.index_client
        self.db_driver = kwargs.pop("db_driver", None) or current_app.db
        self.config = kwargs.pop("flask_config", None) or current_app.config
        #: Create a transaction log, this will be created and committed to the
        #: database during claim_transaction_log()
        self.transaction_id = kwargs.pop("transaction_id", None)
        if kwargs:
            self.logger.warning("Unused arguments: %s", list(kwargs.keys()))

        self.graph_validator = validators.GDCGraphValidator()
        self.transactional_errors = []

        self.logger.info(
            "User ID %s: new transaction for project %s",
            auth.current_user.id,
            self.project_id,
        )

        #: Verify that this transaction is allowed
        self.assert_project_state()
        try:
            # BulkUploadTransaction has @property(entities)
            self.entities = []
        except AttributeError:
            pass

    @property
    def session(self):
        """Wrap current database session."""
        return self.db_driver.current_session()

    @property
    def project_id(self):
        """Return the project id."""
        return "{}-{}".format(self.program, self.project)

    @property
    def json(self):
        """Return the json representation (wraps ``base_json``)."""
        return self.base_json

    @property
    def base_json(self):
        """
        Return attributes in dictionary form.
        """
        return {
            "transaction_id": self.transaction_id,
            "success": self.success,
            "entity_error_count": self.entity_error_count,
            "transactional_error_count": self.transactional_error_count,
            "entities": self.entity_responses,
            "code": self.status_code,
            "message": self.message,
            "transactional_errors": self.transactional_errors,
        }

    @property
    def status_code(self):
        """Return status code according to ``self.success``."""
        if self.success:
            return 200
        else:
            return 400

    @property
    def valid_entities(self):
        """
        Return a list of entities that (up to this point) have no recorded
        errors.
        """
        return [e for e in self.entities if e.is_valid]

    @property
    def entity_errors(self):
        """
        Return the error JSON for each entity that is up to this point
        unsuccessful.
        """
        return [e.errors for e in self.entities if not e.is_valid]

    @property
    def entity_error_count(self):
        """
        Return only the number of errors recorded for all of the the
        transaction's entities.
        """
        return len(self.entity_errors)

    @property
    def error_count(self):
        """
        Return the **total** error count: the sum of transactional and entity
        error counts.
        """
        return self.entity_error_count + len(self.transactional_errors)

    @property
    def transactional_error_count(self):
        """
        Return the number of transactional errors recorded up to this point.

        Transactional errors are errors that are not specific to an individual
        entity.
        """
        return len(self.transactional_errors)

    @property
    def success(self):
        """
        Return true if the transaction has been successful so far. A
        transaction is considered successful if it or its entities have not
        recorded errors up to the point ``.success`` is referenced.
        """
        return self.error_count == 0

    @property
    def message(self):
        """Return a string describing the current state of the transaction."""
        if self.success and not self.dry_run:
            return "Transaction successful with {} entities".format(len(self.entities))

        elif self.success and self.dry_run:
            return "Dry run successful with {} entities".format(len(self.entities))
        else:
            return "Transaction failed."

    @property
    def entity_responses(self):
        """Return a list of JSON response objects generated by each entity."""
        return [entity.json for entity in self.entities]

    @property
    def nodes(self):
        """
        Return a list containing the node for all successfully instantiated
        entities.
        """
        return [entity.node for entity in self.entities if entity.node]

    def assert_project_state(self):
        """Assert that the transaction is allowed given the Project.state."""
        project = utils.lookup_project(self.db_driver, self.program, self.project)
        state = project.state
        if state not in self.REQUIRED_PROJECT_STATES:
            states = " or ".join(self.REQUIRED_PROJECT_STATES)
            msg = (
                "Project is in state '{}', which prevents {}. In order to"
                " perform this action, the project must be in state <{}>."
            )
            raise UserError(msg.format(state, flask.request.path, states))

    def __enter__(self):
        """Called when entering a transaction context.

        This method sets:
        - transaction_log

        """
        self.logger.info("Entering {}".format(self))
        self.claim_transaction_log()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Called when exiting a transaction context.

        This method will always rollback any uncommitted changes in
        the session to avoid them being committed as the session scope
        is exited.

        Because this function always rollsback, the programmer should
        be sure to call self.session.commit() when they are confident
        they are writing a valid transaction to the database.

        """

        self.rollback()

    def claim_transaction_log(self):
        """Creates a new, default transaction log and writes it to the
        database in a separate session.

        This allows the transaction log and transaction mutations to be
        separate transactions and clients to view the transaction
        before completion.

        :returns: The integer id of the transaction

        """

        if self.transaction_id is not None:
            return  # This transaction already has a transaction_log

        with self.clean_session() as tx_session:
            transaction_log = tx_session.merge(self.new_transaction_log())
            tx_session.commit()
            self.transaction_id = transaction_log.id

    @contextmanager
    def fetch_transaction_log(self):
        """Look up external state of transaction log."""
        with self.clean_session():
            yield (
                self.db_driver.nodes(models.submission.TransactionLog).get(
                    self.transaction_id
                )
            )

    def set_transaction_log_state(self, state):
        """
        Transition the transaction_log.state to param:`state` in a clean
        session.
        """
        with self.fetch_transaction_log() as tx_log:
            tx_log.state = state

    @contextmanager
    def clean_session(self):
        """
        Create a new nested session context independent of existing sessions.
        """
        with self.db_driver.session_scope(can_inherit=False) as session:
            yield session

    @staticmethod
    def new_stub_transaction_document():
        """
        Create a stub document.

        This is more an artifact of bulk transactions. Bulk transactions
        include multiple documents in one transaction. The API attempts to keep
        it as uniform as possible with the normal transaction response by
        simply creating a response per document in the bulk transaction.
        """
        return models.submission.TransactionDocument(
            name=None, doc_format="N/A", doc=""
        )

    def new_transaction_log(self):
        """Returns a new, default transaction log"""
        return models.submission.TransactionLog(
            program=self.program,
            project=self.project,
            role=self.role,
            is_dry_run=self.dry_run,
            canonical_json=[],
            state=TX_LOG_STATE_PENDING,
        )

    def record_error(self, message, **kwargs):
        """
        Record an error message.

        Args:
            message (str): a message explaining what went wrong

        Return:
            None
        """
        self.transactional_errors.append(dict(message=message, **kwargs))

    def record_errors_for_empty_transaction(self):
        """Record transactional_errors for empty subtransactions."""
        if not self.entities:
            self.transactional_errors.append("Nothing to submit")

    def rollback(self):
        """Erase all changes in the current session."""
        self.logger.info("{}: rolling back transaction".format(self))
        return self.session.rollback()

    def commit(self, assert_has_entities=True):
        """
        Conditionally write to the database.

        If ``assert_has_entities`` and there are no entities, record an error
        and return without committing.

        If the transaction is a dry run, do not write to database (rollback),
        but return without recording an error.

        If successful, attempt to flush changes, transaction log and commit.
        """

        self.logger.info("{}: committing".format(self))

        if assert_has_entities:
            self.record_errors_for_empty_transaction()

        self.write_transaction_log()

        if not self.success:
            self.set_transaction_log_state(TX_LOG_STATE_FAILED)
            return self.session.rollback()

        if self.dry_run:
            self.set_transaction_log_state(TX_LOG_STATE_SUCCEEDED)
            return self.rollback()

        try:
            self.session.commit()
            self.set_transaction_log_state(TX_LOG_STATE_SUCCEEDED)
            self.logger.info("{}: committed".format(self))
        except Exception as e:  # pylint: disable=broad-except
            self.logger.exception(e)
            msg = "Unable to write to database, please try again"
            self.transactional_errors.append(msg)
            self.set_transaction_log_state(TX_LOG_STATE_ERRORED)
            self.session.rollback()

    def record_user_error(self, exception):
        """
        Centralized management for transaction user errors.

        Args:
            exception (Exception): the Exception that was raised

        Log the exception message and return it to the user.
        """
        self.logger.exception(exception)
        if str(exception):
            self.transactional_errors.append(str(exception))
        self.rollback()
        self.set_transaction_log_state(TX_LOG_STATE_FAILED)
        self.write_transaction_log()
        self.session.commit()

    def record_internal_error(self, exception):
        """
        Centralized management for transaction internal errors.

        Args:
            exception (Exception): the Exception that was raised

        Log the exception message but do not return it to the user.
        """
        self.logger.exception(exception)
        self.transactional_errors.append(MESSAGE_500)
        self.rollback()
        self.set_transaction_log_state(TX_LOG_STATE_ERRORED)
        self.write_transaction_log()
        self.session.commit()

    def get_transaction_timestamp(self):
        """
        Return the timestamp that was written on entities. If the flush was
        lazy (no entities, etc), create a new one.
        """
        self.session.flush()
        if self.session._flush_timestamp:
            # Grab the timestamp of the previous nodes flush
            timestamp = self.session._flush_timestamp
        else:
            # Nothing was dirty so flush was lazy; grab a new timestamp.
            selection = self.session.execute("SELECT CURRENT_TIMESTAMP")
            timestamp = list(selection)[0][0]
        return timestamp.isoformat("T")

    def write_transaction_log(self):
        """
        Write the entity snapshots and response json to the Transaction*
        models.
        """
        timestamp = self.get_transaction_timestamp()
        with self.fetch_transaction_log() as tx_log:
            tx_log.submitter = auth.current_user.username
            for entity in self.entities:
                if not entity.node or isinstance(entity.node, MissingNode):
                    continue
                snapshot = models.submission.TransactionSnapshot()
                snapshot.entity_id = entity.node.node_id
                snapshot.old_props = entity.old_props
                snapshot.new_props = entity.node.props
                snapshot.action = entity.action
                snapshot.transaction = tx_log
                self.session.add(snapshot)
            # Must flush to database to create id
            tx_log.timestamp = timestamp
            if tx_log.documents:
                tx_log.documents[0].response_json = self.json
