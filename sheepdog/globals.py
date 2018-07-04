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
    'reassign': 'admin',
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
            True if dictionary.settings == None
            else dictionary.settings.get('enable_case_cache', True)
        )
    except (AttributeError, KeyError, TypeError):
        return True

def dictionary_version():
    """
    Return the dictionary version. NOTE that the dictionary must be initialized
    first!

    .. note::

        This function assumes that the dictionary has already been initialized.
        The except/return None behavior is to, for example, allow Sphinx to
        still import/run individual modules without raising errors.
    """
    from sheepdog import dictionary
    try:
        return (
            'Unknown' if dictionary.settings == None
            else dictionary.settings.get('_dict_version', 'Unknown')
        )
    except (AttributeError, KeyError, TypeError):
        return True

def dictionary_commit():
    """
    Return the dictionary commit. NOTE that the dictionary must be initialized
    first!

    .. note::

        This function assumes that the dictionary has already been initialized.
        The except/return None behavior is to, for example, allow Sphinx to
        still import/run individual modules without raising errors.
    """
    from sheepdog import dictionary
    try:
        return (
            'Unknown' if dictionary.settings == None
            else dictionary.settings.get('_dict_commit', 'Unknown')
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
SUCCESS_STATE = 'uploaded'

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

BCR_MAPPING = """
# example:
#   -
#     root: xpath
#     id: xpath
#     properties:
#       property_name:
#         path: xpath
#         type: type
#     datetime_properties:
#       datetime_prop_name:
#         day: xpath
#         month: xpath
#         year: xpath
#     edges:
#       edge_name: xpath
#     edge_properties:
#       edge_name:
#           prop_name:
#             path: xpath
#             type: type
#     edge_datetime_properties:
#       edge_name:
#           datetime_prop_name:
#             day: xpath
#             month: xpath
#             year: xpath

aliquot:
  -
    root: //bio:aliquot
    id: .//bio:bcr_aliquot_uuid
    edges:
      analytes: ancestor::bio:analyte/bio:bcr_analyte_uuid
    edges_by_property:
      centers:
        code: .//bio:center_id
    edge_properties:
      centers:
        plate_id:
          path: ./bio:plate_id
          type: str
        plate_row:
          path: ./bio:plate_row
          type: str
        plate_column:
          path: ./bio:plate_column
          type: str
        shipment_center_id:
          path: ./bio:center_id
          type: str
        shipment_reason:
          path: ./bio:shipment_reason
          type: str
    edge_datetime_properties:
      centers:
        shipment_datetime:
          day: ./bio:day_of_shipment
          month: ./bio:month_of_shipment
          year: ./bio:year_of_shipment
    properties:
      submitter_id:
        path: ./bio:bcr_aliquot_barcode
        type: str
      source_center:
        path: ./bio:source_center
        type: str
      amount:
        path: ./bio:amount
        type: float
      concentration:
        path: ./bio:concentration
        type: float

analyte:
  -
    root: //bio:analyte
    id: .//bio:bcr_analyte_uuid
    edges:
      portions: ancestor::bio:portion/bio:bcr_portion_uuid
    properties:
      submitter_id:
        path: ./bio:bcr_analyte_barcode
        type: str
      analyte_type_id:
        path: ./bio:analyte_type_id
        type: str
      analyte_type:
        path: ./bio:analyte_type
        type: str
      concentration:
        path: ./bio:concentration
        type: float
      amount:
        path: ./bio:amount
        type: float
      a260_a280_ratio:
        path: ./bio:a260_a280_ratio
        type: float
      well_number:
        path: ./bio:well_number
        type: str
      spectrophotometer_method:
        path: ./bio:spectrophotometer_method
        type: str

portion:
  -
    root: //bio:portion
    id: .//bio:bcr_portion_uuid
    edges:
      samples: ancestor::bio:sample/bio:bcr_sample_uuid
    properties:
      submitter_id:
        path: ./bio:bcr_portion_barcode
        type: str
      portion_number:
        path: ./bio:portion_number
        type: str
      weight:
        path: ./bio:weight
        type: float
      is_ffpe:
        path: ./bio:is_ffpe
        type: bool
    datetime_properties:
      creation_datetime:
        day: ./bio:day_of_creation
        month: ./bio:month_of_creation
        year: ./bio:year_of_creation
  -
    root: //bio:shipment_portion
    id: .//bio:bcr_shipment_portion_uuid
    edges:
      samples: ancestor::bio:sample/bio:bcr_sample_uuid
    edges_by_property:
      centers:
        code: .//bio:center_id
    edge_properties:
      centers:
        plate_id:
          path: ./bio:plate_id
          type: str
        plate_row:
          path: ./bio:plate_row
          type: str
        plate_column:
          path: ./bio:plate_column
          type: str
        shipment_center_id:
          path: ./bio:center_id
          type: str
        shipment_reason:
          path: ./bio:shipment_reason
          type: str
    edge_datetime_properties:
      centers:
        shipment_datetime:
          day: ./bio:shipment_portion_day_of_shipment
          month: ./bio:shipment_portion_month_of_shipment
          year: ./bio:shipment_portion_year_of_shipment
    properties:
      submitter_id:
        path: ./bio:shipment_portion_bcr_aliquot_barcode
        type: str
      portion_number:
        path: ./bio:portion_number
        type: str
      weight:
        path: ./bio:weight
        type: float
      is_ffpe:
        path: ./bio:is_ffpe
        type: bool
    datetime_properties:
      creation_datetime:
        day: ./bio:shipment_portion_day_of_shipment
        month: ./bio:shipment_portion_month_of_shipment
        year: ./bio:shipment_portion_year_of_shipment

sample:
  -
    root: //bio:sample
    id: .//bio:bcr_sample_uuid
    edges:
      cases: ancestor::bio:patient/shared:bcr_patient_uuid
    properties:
      submitter_id:
        path: ./bio:bcr_sample_barcode
        type: str
      sample_type_id:
        path: ./bio:sample_type_id
        type: str
      sample_type:
        path: ./bio:sample_type
        type: str
      tumor_code_id: null
      tumor_code: null
      longest_dimension:
        path: ./bio:longest_dimension
        type: str
      intermediate_dimension:
        path: ./bio:intermediate_dimension
        type: str
      shortest_dimension:
        path: ./bio:shortest_dimension
        type: str
      initial_weight:
        path: ./bio:initial_weight
        type: float
      current_weight:
        path: ./bio:current_weight
        type: float
      freezing_method:
        path: ./bio:freezing_method
        type: str
      oct_embedded:
        path: ./bio:oct_embedded
        type: str
      time_between_clamping_and_freezing:
        path: ./bio:time_between_clamping_and_freezing
        type: str
      time_between_excision_and_freezing:
        path: ./bio:time_between_excision_and_freezing
        type: str
      is_ffpe:
        path: ./bio:is_ffpe
        type: bool
      pathology_report_uuid:
        path: ./bio:pathology_report_uuid
        type: str
      days_to_collection:
        path: ./bio:days_to_collection
        type: int
      days_to_sample_procurement:
        path: ./bio:days_to_procurement
        type: int

case:
  -
    root: //*[local-name()='patient']
    id: .//shared:bcr_patient_uuid
    edges_by_property:
      tissue_source_sites:
        code: ./shared:tissue_source_site
      projects:
        code: //admin:admin/admin:disease_code
    properties:
      submitter_id:
        path: ./shared:bcr_patient_barcode
        type: str

slide:
  -
    root: //bio:slide
    id: .//shared:bcr_slide_uuid
    edges:
      portions: ancestor::bio:portion/bio:bcr_portion_uuid
    properties:
      submitter_id:
        path: ./shared:bcr_slide_barcode
        type: str
      section_location:
        path: ./bio:section_location
        type: str
      number_proliferating_cells:
        path: ./bio:number_proliferating_cells
        type: str
      percent_tumor_cells:
        path: ./bio:percent_tumor_cells
        type: float
      percent_tumor_nuclei:
        path: ./bio:percent_tumor_nuclei
        type: float
      percent_normal_cells:
        path: ./bio:percent_normal_cells
        type: float
      percent_necrosis:
        path: ./bio:percent_necrosis
        type: float
      percent_stromal_cells:
        path: ./bio:percent_stromal_cells
        type: float
      percent_inflam_infiltration:
        path: ./bio:percent_inflam_infiltration
        type: float
      percent_lymphocyte_infiltration:
        path: ./bio:percent_lymphocyte_infiltration
        type: float
      percent_monocyte_infiltration:
        path: ./bio:percent_monocyte_infiltration
        type: float
      percent_granulocyte_infiltration:
        path: ./bio:percent_granulocyte_infiltration
        type: float
      percent_neutrophil_infiltration:
        path: ./bio:percent_neutrophil_infiltration
        type: float
      percent_eosinophil_infiltration:
        path: ./bio:percent_eosinophil_infiltration
        type: float
"""
