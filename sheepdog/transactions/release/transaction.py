from sheepdog import utils, models
from sheepdog.transactions.transaction_base import TransactionBase
from sheepdog.utils.versioning import IndexVersionHelper


class ReleaseTransaction(TransactionBase):

    REQUIRED_PROJECT_STATES = [
        'open',
        'review',
        'submitted',
        'processing',
    ]

    def __init__(self, **kwargs):
        super(ReleaseTransaction, self).__init__(role='release', **kwargs)
        self.released_count = 0
        self.versioning_helper = IndexVersionHelper(self.indexd)

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
            return 'Dry run successful. {} project files would have been released.'.format(self.released_count)
        else:
            return 'Release transaction failed.'

    def get_latest_release_number(self):
        # TODO: verify how to retrieve latest release node
        release_node = self.db_driver.nodes(models.DataRelease).props(released=False)

        release_version = "{}.{}".format(release_node.major_version, release_node.minor_version)
        return release_version

    def is_project_released(self, total_unreleased):
        return self.released_count == total_unreleased

    def take_action(self):
        """Attempt to transition the current project state to ``release``."""
        project = utils.lookup_project(self.db_driver, self.program, self.project)
        self.entities = []
        if project.released:
            return self.record_error('Project is already released.')

        if project.releasable is not True:
            message = 'Project is not releasable. '\
                      'Project must be submitted at least once first.'
            return self.record_error(message)

        # performing release action
        project_id = "{}-{}".format(self.program, self.project)
        release_number = self.get_latest_release_number()

        submitted_nodes = self.db_driver.nodes().props(project_id=project_id, state="submitted")
        for node in submitted_nodes:
            _, release_no = self.versioning_helper.release_node(release_number=release_number, node_id=node.id,
                                                                dry_run=self.dry_run)
            if release_no == release_number:
                node.props["state"] = "released"
                self.entities.append(node)
                self.released_count += 1

        if self.is_project_released(len(submitted_nodes)):
            project.released = True
        self.commit()
