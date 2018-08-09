# pylint: disable=unsubscriptable-object
"""
Contains values for global constants.
"""

import re
import uuid


#: Regex to match a Program or Project uuid.
REGEX_UUID = re.compile(
    r'^[a-fA-F0-9]{8}'
    r'-[a-fA-F0-9]{4}'
    r'-[a-fA-F0-9]{4}'
    r'-[a-fA-F0-9]{4}'
    r'-[a-fA-F0-9]{12}$'
)

FLAG_IS_ASYNC = 'async'

DELIMITERS = {'csv': ',', 'tsv': '\t'}
SUPPORTED_FORMATS = ['csv', 'tsv', 'json']

ROLES = {
    'ADMIN': 'admin',
    'CREATE': 'create',
    'DELETE': 'delete',
    'DOWNLOAD': 'download',
    'GENERAL': '_member_',
    'READ': 'read',
    'RELEASE': 'release',
    'UPDATE': 'update',
}

PERMISSIONS = {
    'list_parts': 'read',
    'abort_multipart': 'create',
    'get_file': 'download',
    'complete_multipart': 'create',
    'initiate_multipart': 'create',
    'upload': 'create',
    'upload_part': 'create',
    'delete': 'delete',
}


TEMPLATE_NAME = 'submission_templates.tar.gz'

PROGRAM_SEED = uuid.UUID('85b08c6a-56a6-4474-9c30-b65abfd214a8')
PROJECT_SEED = uuid.UUID('249b4405-2c69-45d9-96bc-7410333d5d80')

UNVERIFIED_PROGRAM_NAMES = ['TCGA']
UNVERIFIED_PROJECT_CODES = []

# File upload states
#: State a file should be put in given an error.
ERROR_STATE = 'error'


def case_cache_enabled():
    """
    Return if the case cache is enabled or not. NOTE that the dictionary must be initialized
    first!

    .. note::

        This function assumes that the dictionary has already been initialized.
        The except/return None behavior is to, for example, allow Sphinx to
        still import/run individual modules without raising errors.
    """
    from sheepdog import dictionary
    try:
        return (
            True if dictionary.settings is None
            else dictionary.settings.get('enable_case_cache', True)
        )
    except (AttributeError, KeyError, TypeError):
        return True


def submitted_state():
    """
    Return the initial file state. NOTE that the dictionary must be initialized
    first!

    This would be a global defined as:

    .. code-block:: python

        SUBMITTED_STATE = (
            dictionary.resolvers['_definitions.yaml'].source['file_state']['default']
        )

    but the dictionary must be initialized first, so this value cannot be used
    before that.

    .. note::

        This function assumes that the dictionary has already been initialized.
        The except/return None behavior is to, for example, allow Sphinx to
        still import/run individual modules without raising errors.
    """
    from sheepdog import dictionary
    try:
        return (
            dictionary.resolvers['_definitions.yaml']
            .source['file_state']['default']
        )
    except (AttributeError, KeyError, TypeError):
        return None


#: State file enters when user begins upload.
UPLOADING_STATE = 'uploading'
#: State file enters when user completes upload.
UPLOADED_STATE = 'uploaded'

#: This is a list of states that an entity must be in to allow deletion.
ALLOWED_DELETION_STATES = [
    'validated',
]

#: Allow dry_run transactions to be committed (in a new transaction)
#: if the TransactionLog.state is in the following
STATES_COMITTABLE_DRY_RUN = {'SUCCEEDED'}

MEMBER_DOWNLOADABLE_STATES = ['submitted', 'processing', 'processed']
SUBMITTER_DOWNLOADABLE_STATES = [
    'uploaded', 'validating', 'validated', 'error', 'submitted', 'processing',
    'processed'
]

UPLOADING_PARTS = [
    'upload_part', 'complete_multipart', 'list_parts', 'abort_multipart'
]

# Transaction Logs
#: The transaction succeeded without user or system error.  If the
#: transaction was a non-dry_run mutation, then the result should be
#: represented in the database
TX_LOG_STATE_SUCCEEDED = 'SUCCEEDED'
#: The transaction failed due to user error
TX_LOG_STATE_FAILED = 'FAILED'
#: The transaction failed due to system error
TX_LOG_STATE_ERRORED = 'ERRORED'
#: The transaction is sill pending or a fatal event ended the job
#: before it could report an ERROR status
TX_LOG_STATE_PENDING = 'PENDING'

#: Message to provide for internal server errors.
MESSAGE_500 = 'Internal server error. Sorry, something unexpected went wrong!'

#: These categories should all have a ``state`` associated with each type
ENTITY_STATE_CATEGORIES = [
    'biospecimen',
    'clinical',
    'data_file',
    # 'cases' => cases are currently `admin` but are manually included
    #      in submission
    # 'annotations' => cases are currently `TBD` but are manually
    #      included in submission
]
#: Possible entity.state transitions
#: { to_state: from_state }
ENTITY_STATE_TRANSITIONS = {
    'submitted': ['validated', None],
}
#: The key that specifies the high level state that a file is in the
#: pipeline
FILE_STATE_KEY = 'file_state'
#: Possible data_file.file_state transitions
#: { to_state: from_state }
FILE_STATE_TRANSITIONS = {
    'submitted': ['validated'],
}
#: The key that specifies the high level state that an entity is in the
#: release process.
STATE_KEY = 'state'

# The auth roles required to take actions
ROLE_SUBMIT = 'release'
ROLE_REVIEW = 'release'
ROLE_OPEN = 'release'

SUBMITTABLE_FILE_STATES = FILE_STATE_TRANSITIONS['submitted']
SUBMITTABLE_STATES = ENTITY_STATE_TRANSITIONS['submitted']

# Async scheduling configuration
ASYNC_MAX_Q_LEN = 128
ERR_ASYNC_SCHEDULING = (
    'The API is currently under heavy load and currently has too many'
    ' asynchronous tasks. Please try again later.'
)

# Categories of nodes considered 'file node'
DATA_FILE_CATEGORIES = ['data_file', 'metadata_file', 'index_file']

# Used to set 'type' in indexd_document.urls_metadata[<url>] to determine
# a primary URL if multiple URLs are present (e.g. backup)
PRIMARY_URL_TYPE = 'cleversafe'

# TODO: This should probably go into the dictionary and be
# read from there. For now, these are the only nodes that will
# be allowed to be set to 'open'.
POSSIBLE_OPEN_FILE_NODES = [
    'biospecimen_supplement',
    'clinical_supplement',
    'copy_number_segment',
    'gene_expression'
    'masked_somatic_mutation',
    'methylation_beta_value',
    'mirna_expression',
    'file',
]

# A list of indexd file states for which nodes can still be modified
UPDATABLE_FILE_STATES = [
    'registered',
    'uploading',
    'uploaded',
    'validating',
    'error',
]

# Below is the list of node states that are treated as "released"
RELEASED_NODE_STATES = [
    'released',
    'live',
]
