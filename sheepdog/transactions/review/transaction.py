from sheepdog import utils
from sheepdog.transactions.transaction_base import TransactionBase


class ReviewTransactionBase(TransactionBase):

    role = None
    required_project_flags  = {
        'in_review': [False, None],
        'submission_enabled': [True],
    }

    def __init__(self, **kwargs):
        super(ReviewTransactionBase, self).__init__(role=self.role, **kwargs)

    @property
    def to_state(self):
        """This is the state the project will be in if successful"""
        raise NotImplementedError()

    def write_transaction_log(self):
        """Save a log noting this project was opened"""

        with self.fetch_transaction_log() as tx_log:
            tx_log.documents = [self.new_stub_transaction_document()]

        super(ReviewTransactionBase, self).write_transaction_log()

    @property
    def message(self):
        """The human-readable message to put in the response JSON

        :returns: A string message

        """

        if self.success and not self.dry_run:
            return ("Successfully transitioned project state to '{}'"
                    .format(self.to_state))

        elif self.success and self.dry_run:
            return ("Dry run successful. Would have successfully "
                    "transitioned project state to '{}'".format(self.to_state))

        else:
            return 'Transaction failed.'

    def take_action(self):
        """For the current project, attempt to transition all nodes from their
        current states to ``open``

        :returns: None

        """

        project = utils.lookup_project(self.db_driver, self.program, self.project)
        project.state = self.to_state
        self.commit(assert_has_entities=False)


class ReviewTransaction(ReviewTransactionBase):
    """Mark a Project `review` to prevent mutation"""

    REQUIRED_PROJECT_STATES = ['open']
    role = 'review'

    @property
    def to_state(self):
        return 'review'


class OpenTransaction(ReviewTransactionBase):
    """Mark a Project `open` to allow mutation"""

    REQUIRED_PROJECT_STATES = ['review']
    role = 'open'

    @property
    def to_state(self):
        return 'open'
