from sheepdog import utils
from sheepdog.transactions.transaction_base import TransactionBase


class ReleaseTransaction(TransactionBase):

    REQUIRED_PROJECT_STATES = [
        'open',
        'review',
        'submitted',
        'processing',
    ]

    # TODO: fill this out properly based on projects
    required_project_flags  = {
        'submission_enabled': [True],
    }

    def __init__(self, **kwargs):
        super(ReleaseTransaction, self).__init__(role='release', **kwargs)

    def write_transaction_log(self):
        """Save a log noting this project was opened."""
        with self.fetch_transaction_log() as tx_log:
            tx_log.documents = [self.new_stub_transaction_document()]
        super(ReleaseTransaction, self).write_transaction_log()

    @property
    def message(self):
        """Return human-readable message to put in the response JSON."""
        if self.success and not self.dry_run:
            return 'Successfully released project'
        elif self.success and self.dry_run:
            return 'Dry run successful. Would have released project.'
        else:
            return 'Release transaction failed.'

    def take_action(self):
        """Attempt to transition the current project state to ``release``."""
        project = utils.lookup_project(self.db_driver, self.program, self.project)
        if project.released:
            return self.record_error('Project is already released.')

        if project.releasable is not True:
            message = 'Project is not releasable. '\
                      'Project must be submitted at least once first.'
            return self.record_error(message)

        project.released = True
        self.commit(assert_has_entities=False)
