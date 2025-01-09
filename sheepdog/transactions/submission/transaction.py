import envelopes
import psqlgraph
import sqlalchemy
from flask import current_app as capp

from sheepdog import utils
from sheepdog.auth import authorize
from sheepdog.errors import AuthZError
from sheepdog.globals import (
    ENTITY_STATE_CATEGORIES,
    FLAG_IS_ASYNC,
    ROLE_SUBMIT,
    STATE_KEY,
    SUBMITTABLE_STATES,
)
from sheepdog.transactions.entity_base import EntityErrors
from sheepdog.transactions.submission.entity import SubmissionEntity
from sheepdog.transactions.transaction_base import TransactionBase


class SubmissionTransaction(TransactionBase):
    """Models a transaction to mark all nodes in a project submitted."""

    REQUIRED_PROJECT_STATES = ["review"]

    #: Don't mark these classes submitted
    SKIPPED_CLASSES = ["case", "annotation"]

    def __init__(self, smtp_conf=None, **kwargs):
        super(SubmissionTransaction, self).__init__(role="submit", **kwargs)

        self.app_config = capp.config
        if utils.should_send_email(self.app_config):
            self.smtp_conf = smtp_conf

        try:
            program, project = self.transaction.project_id.split("-", 1)
            authorize(program, project, [ROLE_SUBMIT])
        except AuthZError:
            return self.record_error(
                "You do not have submit permission for project {}".format(
                    self.project_id
                ),
                type=EntityErrors.INVALID_PERMISSIONS,
            )

        self.project_node = utils.lookup_project(
            self.db_driver, self.program, self.project
        )

    @property
    def json(self):
        """Generates the current response JSON

        :returns: dict response

        """

        return dict(
            self.base_json, **{"submitted_entity_count": self.submitted_entity_count}
        )

    @property
    def message(self):
        """The human-readable message to put in the response JSON

        :returns: A string message

        """

        if self.success and not self.dry_run:
            return "Successfully submitted {} entities.".format(len(self.entities))
        elif self.success and self.dry_run:
            return "Dry run successful. Would have submitted {} entities.".format(
                len(self.entities)
            )
        else:
            return "Submit transaction failed."

    @property
    def submitted_entity_count(self):
        """Returns the number of entities if submission is successful else 0."""

        if not self.success:
            return 0
        else:
            return len(self.entities)

    def lookup_submittable_nodes(self):
        """
        Return a list of all nodes with current ``project_id`` that have a
        possible transition to state `submitted`.

        Query logic (given transitions as of 2016-02-02):

        Return all nodes n (each of type N), that:

            1. Have n.state in 'validated'

        Return:
            a list of nodes
        """
        nodes = []

        for cls in psqlgraph.Node.get_subclasses():

            skip_this_cls = (
                cls._dictionary["category"] not in ENTITY_STATE_CATEGORIES
                and cls.label not in self.SKIPPED_CLASSES
            )
            if skip_this_cls:
                continue

            _project_id = cls._props["project_id"]
            _node_state = cls._props[STATE_KEY]

            filter_project = _project_id.astext == self.project_id
            filter_state = sqlalchemy.or_(
                _node_state.astext.in_(SUBMITTABLE_STATES), _node_state is None
            )

            nodes += (
                self.db_driver.nodes(cls)
                .filter(filter_project)
                .filter(filter_state)
                .all()
            )

        self.logger.info("Found {} nodes to submit".format(len(nodes)))
        return nodes

    def take_action(self):
        """
        For the current project, attempt to transition all nodes from their
        current states to ``submitted``.
        """
        self.assert_project_state()
        nodes = self.lookup_submittable_nodes()
        self.entities = [SubmissionEntity(self, n) for n in nodes]
        for entity in self.entities:
            entity.submit()

        project_node = self.session.merge(self.project_node)
        project_node.state = "submitted"
        project_node.releasable = True

        if self.success and utils.should_send_email(self.app_config):
            self.send_submission_notification_email()

        self.commit()

    def send_submission_notification_email(self):
        """Sends an email notification

        This is used in the GDC to notify the user services team
        about submitting a project
        """

        from_addr = self.app_config.get("EMAIL_FROM_ADDRESS")
        to_addr = self.app_config.get("EMAIL_SUPPORT_ADDRESS")
        preformatted = self.app_config.get("EMAIL_NOTIFICATION_SUBMISSION")
        subject = (
            "[SUBMISSION] Project {project_id} has been submitted by {user}".format(
                project_id=self.project_id, user=self.user.username
            )
        )

        number_of_cases = 0
        number_of_submitted_files = 0
        experimental_strategies = set()
        for entity in self.entities:
            if entity.node.label == "case":
                number_of_cases += 1

            if utils.is_node_file(entity.node):
                ex_strategy = entity.node.props.get("experimental_strategy")
                if ex_strategy:
                    experimental_strategies.add(ex_strategy)

        # Construct body
        text_body = preformatted.format(
            project_id=self.project_id,
            user_id=self.user.username,
            number_of_cases=number_of_cases,
            number_of_submitted_files=number_of_submitted_files,
            experimental_strategies=",".join(experimental_strategies),
        )

        # Construct email
        envelope = envelopes.Envelope(
            from_addr=from_addr, to_addr=to_addr, subject=subject, text_body=text_body
        )

        description = "email '{}' to {}".format(subject, to_addr)
        self.logger.info("Sending " + description)

        # Send email (synchronous)
        connection = envelopes.SMTP(**self.smtp_conf)
        connection.send(envelope)

        self.logger.info("Sent " + description)

    def write_transaction_log(self):
        """Save a log noting this project was opened."""
        with self.fetch_transaction_log() as tx_log:
            tx_log.documents = [self.new_stub_transaction_document()]
        super(SubmissionTransaction, self).write_transaction_log()
