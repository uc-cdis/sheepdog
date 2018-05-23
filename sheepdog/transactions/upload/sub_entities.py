"""
Subclasses for UploadEntity that handle different types of
uploaded entities.
"""
import uuid

import psqlgraph
from indexclient.client import Document
from sheepdog.utils import (
    lookup_project,
    is_project_public,
    generate_s3_url,
    get_indexd_state,
)

from sheepdog.transactions.entity_base import EntityErrors
from sheepdog.transactions.upload.entity import (
    UploadEntity,
    lookup_node,
)
from sheepdog.globals import DATA_FILE_CATEGORIES


class NonFileUploadEntity(UploadEntity):
    """
    Deals with non-file-like entities.

    At the moment, there is no non-file specific logic for upload entities.

    This was created for clarity and future extensibility in the case that
    non-file-like upload entity logic is required.
    """

    def __init__(self, *args, **kwargs):
        super(NonFileUploadEntity, self).__init__(*args, **kwargs)


class FileUploadEntity(UploadEntity):
    """
    Deals with file-like entities.

    As such, this class extends the general UploadEntity class by
    handling the interaction to the index service.

    Like it's parent class:
    The lifespan of a FileUploadEntity should include:
       #. parse
       #. pre_validate
       #. instantiate
       #. post_validate

    Some things to note about data files and the index service:
    - submitter_id/project should be unique per graph node
    - submitter_id/project are used to create an alias in the index service to
        a data file
    - when sheepdog attempts to find out if a file exists in index service it
      has TWO methods of determining
        FIXME At the moment, there is a 1:1 mapping of id's between graph nodes and
              indexed files. eventually, there'll be a file_id attr in the graph node
        1) If a file uuid exists in the graph node or is provided in the submission,
           we can look up that uuid in index service
        2) The hash/file_size combo should be unique in the index service for each
           file

    Here are a few submission examples and how we currently handle. The
    examples here refer to whether or not an "id" field is included during
    the submission.

    - "id" NOT provided , file NOT in index service
        - A new uuid is generated and used for both the graph and indexed file

    - "id" NOT provided , file in index service
        - uuid from the file in the index service is used for the graph id

    - "id" provided     , file NOT in index service
        - id provided is used for the indexed file

    - "id" provided     , file in index service
        - Make sure the uuids match

    ERROR cases handled:
        - If you attempt to submit the same data again with a different "id"
            it will fail. At the moment we are maintaining a 1:1 between indexed
            files and graph nodes
        - Attempting to submit new data (new file) when the node has already
            been created with a previous file will fail. We don't currently
            allow updating the linkage to a new file since we're forcing
            a 1:1 between uuids in graph and index service
    """
    def __init__(self, *args, **kwargs):
        super(FileUploadEntity, self).__init__(*args, **kwargs)

        # file exists in indexd
        self.file_exists = False
        self.file_by_uuid = None
        self.file_by_hash = None
        self.s3_url = None
        self.urls = []

    def parse(self, doc):
        """
        Parse the given doc and set the instance values accordingly.

        Args:
            doc (dict): the json upload representation

        Return:
            None
        """
        # If a url is included, get it here and remove it from doc so it
        # doesn't get validated
        entity_urls = doc.get('urls')
        if entity_urls:
            self.urls = entity_urls.strip().split(',')
            # remove from the doc since we don't want to validate it
            doc.pop('urls')

        super(FileUploadEntity, self).parse(doc)

    def get_node_create(self, skip_node_lookup=False):
        """
        This is called for a POST operation.

        Return:
            psqlgraph.Node
        """
        self._populate_file_exist_in_index()
        self._set_entity_id()

        # call to super must happen after setting node and file ids here
        node = super(FileUploadEntity, self).get_node_create(
            skip_node_lookup=skip_node_lookup,
        )
        node.acl = self.transaction.get_phsids()

        return node

    def get_node_merge(self):
        """
        This is called for a PUT operation and supports upsert. It will
        lookup an existing node or create one if it doesn't exist.

        Return:
            psqlgraph.Node:
        """
        # entity_id is set to the node_id here
        node = super(FileUploadEntity, self).get_node_merge()

        self._populate_file_exist_in_index()
        self._is_valid_index_id_for_graph()

        # if no indexd record, do not check if file updatable
        if not self.transaction.indexd.get(node.node_id):
            self.file_exists = False
            return node

        file_state = get_indexd_state(
            node.node_id,
            self.s3_url,
            self.transaction.indexd
        )

        # verify that update is allowed
        if not self.is_updatable_file_node(node):
            self.record_error(
                ("This file is already in file_state '{}' and cannot be "
                 "updated. The raw data exists in the file storage "
                 "and modifying the Entity now is unsafe and may cause "
                 "problems for any processes or users consuming "
                 "this data.").format(file_state),
                keys=['file_state'],
                type=EntityErrors.INVALID_PERMISSIONS,
            )

        return node

    def flush_to_session(self):
        """
        Depending on the role and status of the file in the index service,
        register or update an index. Then call parent class's
        flush_to_session.
        """
        # Refresh self.file_exists info
        self._populate_file_exist_in_index()

        if not self.node:
            return

        role = self.action
        try:
            if role == 'create':
                if not self.file_exists:
                    self._register_index()

            elif role == 'version':
                if self.file_exists:
                    self._new_version_index()

            elif role == 'update':
                if self.file_exists:
                    self._update_index()
            else:
                message = 'Unknown role {}'.format(role)
                self.logger.error(message)
                self.record_error(
                    message, type=EntityErrors.INVALID_PERMISSIONS
                )
        except Exception as e:
            self.logger.exception(e)
            self.record_error(str(e))

        # next do node creation
        super(FileUploadEntity, self).flush_to_session()

    def _register_index(self):
        """
        Call the index client for the transaction to register a
        new index record for this entity.
        """

        project_id = self.transaction.project_id
        program = self.transaction.program
        project = self.transaction.project
        submitter_id = self.node._props.get('submitter_id')
        hashes = {'md5': self.node._props.get('md5sum')}
        size = self.node._props.get('file_size')
        file_name = self.node._props.get('file_name')
        alias = "{}/{}".format(project_id, submitter_id)
        metadata = self.get_metadata()

        project_node = lookup_project(
            self.transaction.db_driver,
            self.transaction.program,
            self.transaction.project
        )

        if is_project_public(project_node):
            acls = ['*']
        else:
            acls = self.transaction.get_phsids()

        if not self.urls:
            url = generate_s3_url(
                host=self._config['SUBMISSION']['host'],
                bucket=self._config['SUBMISSION']['bucket'],
                program=program,
                project=project,
                uuid=self.entity_id,
                file_name=file_name,
            )
            self.urls = [url]
            urls_metadata = {url: {'state': 'registered'}}
        else:
            urls_metadata = {url: {'state': 'registered'} for url in self.urls}

        # IndexClient
        self._create_index(did=self.entity_id,
                           hashes=hashes,
                           size=size,
                           urls=self.urls,
                           acl=acls,
                           file_name=file_name,
                           metadata=metadata,
                           urls_metadata=urls_metadata)

        self._create_alias(
            record=alias, hashes=hashes, size=size, release='private'
        )

    def _update_index(self):
        """
        Call the index client for the transaction to update an
        index record for this entity.
        """
        document = self.file_by_uuid

        if self.urls:
            urls_to_add = [url for url in self.urls if url not in document.urls]
            if urls_to_add:
                document.urls.extend(urls_to_add)
                document.patch()

    def _new_version_index(self):
        """
        Call the index client for the transaction to create a new version
        of an already existing index for the old_uuid.
        """

        file_name = self.node._props.get('file_name')

        project_id = self.transaction.project_id
        program = project_id.split('-')[0]
        project = '-'.join(project_id.split('-')[1:])

        urls = self.urls or [
            generate_s3_url(
                host=self._config['SUBMISSION']['host'],
                bucket=self._config['SUBMISSION']['bucket'],
                program=program,
                project=project,
                uuid=self.entity_id,
                file_name=file_name,
            )
        ]

        index_json = dict(
            did=self.entity_id,
            hashes={'md5': self.node._props.get('md5sum')},
            size=self.node._props.get('file_size'),
            file_name=file_name,
            urls=urls,
            acl=self.transaction.get_phsids(),
            metadata=self.get_metadata(),
            form='object',
            urls_metadata={url: {'state': 'registered'} for url in urls}
        )
        new_doc = Document(None, None, index_json)

        self._version_index(current_did=self.old_uuid, new_doc=new_doc)

    def is_updatable_file_node(self, node):
        """
        Check that a node is a file that can be updated. True if:

        #. The node is a file
        #. It has a file_state in the list of states below

        Args:
            node (psqlgraph.Node):

        Return:
            bool: whether the node is an updatable file
        """
        allowed_states = [
            'registered',
            'uploading',
            'uploaded',
            'validating',
        ]
        is_data_file = node._dictionary.get('category') in DATA_FILE_CATEGORIES
        if not is_data_file:
            return False

        file_state = get_indexd_state(
            node.node_id,
            self.s3_url,
            self.transaction.indexd
        )
        return file_state in allowed_states

    def _populate_file_exist_in_index(self):
        """
        Populate self.file_exists (in index service)
        - Will first check provided uuid
        - then (if file hash size uniqueness is enforced) check by hash/size
        """

        did = self.old_uuid or self.entity_id
        self.file_by_hash = self.get_file_from_index_by_hash()
        self.file_by_uuid = self.get_file_from_index_by_uuid(did)

        #######################################################################
        # need to find out if this causes application working outside
        # of context error
        #######################################################################
        if self._config.get('ENFORCE_FILE_HASH_SIZE_UNIQUENESS', True):
            self.file_exists = bool(self.file_by_uuid or self.file_by_hash)
        else:
            self.file_exists = bool(self.file_by_uuid)

    def _set_entity_id(self):
        """
        Set self.entity_id based on information provided
        and whether or not the file exists in the index service.
        """

        if not self.file_exists:
            # generate entity_id if not provided
            if not self.entity_id:
                self.entity_id = str(uuid.uuid4())
        else:
            # If hash and size uniqueness not enforced, do nothing
            if not self._config.get('ENFORCE_FILE_HASH_SIZE_UNIQUENESS', True):
                return

            if self.entity_id:
                # check to make sure that when file exists
                # and an id is provided that they are the same
                # NOTE: record errors are populated in check below
                self._is_valid_index_for_file()
            else:
                # the file exists in indexd and
                # no node id is provided, so attempt to use indexed id
                file_by_hash_index = getattr(self.file_by_hash, 'did', None)

                # ensure that the index we found matches the graph (this will
                # populate record errors if there are any issues)
                if self._is_valid_index_id_for_graph():
                    self.entity_id = file_by_hash_index

    def _is_valid_index_for_file(self):
        """
        Return whether or not uuid provided matches hash/size for file in index.

        Will first check for an indexed file with provided hash/size.
        Will then check for file with given uuid. Then will make sure
        those uuids match. record_errors will be set with any issues
        """
        is_valid = True

        if not self.file_by_hash or not self.file_by_uuid:
            error_message = (
                'Could not find exact file match in index for id: {} '
                'AND `hashes - size`: `{} - {}`. '
            ).format(
                self.entity_id,
                str(self._get_file_hashes()),
                str(self._get_file_size())
            )

            if self.file_by_hash:
                error_message += 'A file was found matching `hash / size` but NOT id.'
            elif self.file_by_uuid:
                error_message += 'A file was found matching id but NOT `hash / size`.'
            else:
                # keep generic error message since both didn't result in a match
                pass

            self.record_error(
                error_message,
                type=EntityErrors.INVALID_VALUE
            )
            is_valid = False

        if (self.file_by_hash and self.file_by_uuid and
                self.file_by_hash.did != self.file_by_uuid.did):
            # both exist but dids are different
            # FIXME: error should be handled different/removed
            #        when we support updating indexed files
            self.record_error(
                'Provided id for indexed file {} does not match the id '
                'for the file discovered in the index by hash/size ('
                'id: {}). Updating a previous index with new file '
                'is currently NOT SUPPORTED.'
                .format(self.file_by_uuid.did, self.file_by_hash.did),
                type=EntityErrors.INVALID_VALUE,
            )
            is_valid = False

        if is_valid:
            is_valid = self._is_valid_index_id_for_graph()

        return is_valid

    def _is_valid_index_id_for_graph(self):
        # Do not check by hash and size if uniqueness is not enforced (for ex. GDC)

        if self._is_replaceable:
            return True

        if not self._config.get('ENFORCE_FILE_HASH_SIZE_UNIQUENESS', True):
            return True

        is_valid = True
        # if a single match exists in the graph, check to see if
        # file exists in index service
        query = lookup_node(
            self.transaction.db_driver,
            self.entity_type,
            self.entity_id,
            self.secondary_keys
        )
        if query.count() == 1:
            if self.file_exists:
                node = query.one()
                file_by_uuid_index = getattr(self.file_by_uuid, 'did', None)
                file_by_hash_index = getattr(self.file_by_hash, 'did', None)

                if ((file_by_uuid_index != node.node_id) or
                        (file_by_hash_index != node.node_id)):
                    self.record_error(
                        'Graph ID and index file ID found in index service do not match, '
                        'which is currently not permitted. Graph ID: {}. '
                        'Index ID: {}. Index ID found using hash/size: {}.'
                        .format(node.node_id, file_by_hash_index, file_by_uuid_index),
                        type=EntityErrors.NOT_UNIQUE,
                    )
                    is_valid = False

        return is_valid

    def get_node_recreate(self):
        """
        Create a new node in the old node's place if it exists in indexd. with
        graph node state = 'released'.
        """

        nodes = lookup_node(
            self.transaction.db_driver,
            self.entity_type,
            self.entity_id,
            self.secondary_keys,
        )

        self.old_uuid = nodes.one().node_id
        self.file_by_uuid = self.get_file_from_index_by_uuid(self.entity_id)
        with self.transaction.db_driver.session_scope() as session:
            for node in nodes:
                session.delete(node)

        # new node id
        self.entity_id = str(uuid.uuid4())

        # Fill in default system property values
        for key, val in self.get_system_property_defaults().iteritems():
            if self.doc.get(key, None) is None:
                self.doc[key] = val

        # Create the node and populate its properties
        cls = psqlgraph.Node.get_subclass(self.entity_type)
        self.logger.debug('Recreating new {}'.format(cls.__name__))
        node = cls(self.entity_id)

        # check if open_acl is requested and the node type can be set open
        if self.doc.get('open_acl', None) and self._config.get('IS_GDC', False):
            if self.entity_type in POSSIBLE_OPEN_FILE_NODES:
                node.acl = [u'open']
            else:
                node.acl = self.transaction.get_phsids()

        self.action = 'version'
        return node

    def get_file_from_index_by_hash(self):
        """
        Return the record entity from index client

        NOTE:
        - Should only ever be called for data and metadata files.
        - If there is already a record matching the hash and size for this
          file, then return none.
        """
        document = None

        # Check if there is an existing record with this hash and size, i.e.
        # this node already has an index record.
        params = self._get_file_hashes_and_size()
        # document: indexclient.Document
        # if `document` exists, `document.did` is the UUID that is already
        # registered in indexd for this entity.
        document = self.transaction.indexd.get_with_params(params)

        return document

    def get_file_from_index_by_uuid(self, uuid):
        """
        Return the record entity from index client

        NOTE:
        - Should only ever be called for data and metadata files.
        - If there is already a record matching the hash and size for this
          file, then return none.

        Args:
            uuid (str): unique digital id for a node in the database
        """

        document = None

        if uuid:
            document = self.transaction.indexd.get(uuid)

        return document

    def _get_file_hashes_and_size(self):
        hashes = self._get_file_hashes()
        size = self._get_file_size()
        return {'hashes': hashes, 'size': size}

    def _get_file_hashes(self):
        return {'md5': self.doc.get('md5sum')}

    def _get_file_size(self):
        size = self.doc.get('file_size')
        return size

    def _create_alias(self, **kwargs):
        return self.transaction.indexd.create_alias(**kwargs)

    def _create_index(self, **kwargs):
        return self.transaction.indexd.create(**kwargs)

    def _version_index(self, **kwargs):
        return self.transaction.indexd.add_version(**kwargs)
