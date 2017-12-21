# -*- coding: utf-8 -*-
"""
gdcapi.resources.submission.constants
----------------------------------

Define variables to be imported to other modules.  These variables
contain things like state transition flows.

"""

from gdcdictionary import gdcdictionary

import re
import uuid

# ======================================================================
# Dictionary Consts

#: Do we have a cache case setting and should we do it?
CACHE_CASES = (
    True if not hasattr(gdcdictionary, 'settings')
    else gdcdictionary.settings.get('enable_case_cache', True)
)


# ======================================================================
# File upload

#: State a file should be put in given an error
ERROR_STATE = 'error'

#: Initial file state
SUBMITTED_STATE = (gdcdictionary.resolvers['_definitions.yaml']
                   .source['file_state']['default'])

#: State file enters when user begins upload
UPLOADING_STATE = 'uploading'

#: State file enters when user completes upload
SUCCESS_STATE = 'uploaded'

# ======================================================================
# Release/Submit workflow

#: UUID seeds for program/project uuid5 generation
uuid_regex = re.compile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")
project_seed = uuid.UUID('249b4405-2c69-45d9-96bc-7410333d5d80')
program_seed = uuid.UUID('85b08c6a-56a6-4474-9c30-b65abfd214a8')


#: This is a list of states that an entity must be in to allow
#: deletion
ALLOWED_DELETION_STATES = [
    "validated",
]

#: This is a list of file_states that a a file must be in to allow
#: deletion
ALLOWED_DELETION_FILE_STATES = [
    SUBMITTED_STATE,
]


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

#: The auth role required to take action actions
ROLE_SUBMIT = 'release'
ROLE_REVIEW = 'release'
ROLE_OPEN = 'release'

#: The key that specifies the high level state that an entity is in the
#: release process
STATE_KEY = 'state'

#: Allow dry_run transactions to be committed (in a new transaction)
#: if the TransactionLog.state is in the following
STATES_COMITTABLE_DRY_RUN = {'SUCCEEDED'}

# ======================================================================
# Formats

FORMAT_JSON = 'JSON'
FORMAT_XML = 'XML'
FORMAT_TSV = 'TSV'
FORMAT_CSV = 'CSV'

# ======================================================================
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

# ======================================================================
# Requests

#: Query param flag for performing transaction in background with
#: early return
FLAG_IS_ASYNC = 'async'
FLAG_IS_DRY_RUN = 'dry_run'

# ======================================================================
# Error messages

ERR_ASYNC_SCHEDULING = 'The API is currently under heavy load and currently has too many asynchronous tasks. Please try again later.'

#: Things go wrong, let's make a message for when they do
MESSAGE_500 = 'Internal server error. Sorry, something unexpected went wrong!'


# ======================================================================
# Async

ASYNC_MAX_Q_LEN = 128
