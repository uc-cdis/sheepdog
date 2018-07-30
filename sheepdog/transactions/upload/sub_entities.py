"""
Subclasses for UploadEntity that handle different types of
uploaded entites.
"""
import uuid

from sheepdog.errors import NoIndexForFileError

from sheepdog import utils
from sheepdog.transactions.entity_base import EntityErrors

from sheepdog.transactions.upload.entity import UploadEntity
from sheepdog.transactions.upload.entity import lookup_node

from sheepdog import dictionary


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
      has a few methods of determining:
        1) If a file uuid exists in the graph node we can look up that uuid in
           index service
        2) If an id is provided in the submission we can look up that uuid in
           index service
        2) The hash/file_size combo provided should be unique in the index
           service for each file

    Handling Relationship Between Graph Node and Indexed File:
        - sheepdog has 2 different methods and supports both in order to be
          backwards compatible
              1) graph node id and indexed file's id must match
                    - 1:1 id matching is enforced
              2) graph node has an `object_id` that maps to the
                 indexed file's id
                    - graph node id and indexed file's id do NOT need to match
                    - 1:1 id matching is NOT enforced
                    - this method is ONLY used when the dictionary schema has
                      an `object_id` property for entities
                          - this makes it backwards-compatible with older
                            dictionaries

    ---------------------------------------------------------------------------

    Here are a few submission examples and how we currently handle situations
    where the `object_id` is NOT in the dictionary, e.g. we are enforcing 1:1.

    The examples here refer to whether or not an "id" field is included during
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
            it will fail.
        - Attempting to submit new data (new file) when the node has already
            been created with a previous file will fail. We don't currently
            allow updating the linkage to a new file since we're forcing
            a 1:1 between uuids in graph and index service
    """
    def __init__(self, *args, **kwargs):
        super(FileUploadEntity, self).__init__(*args, **kwargs)
        self.file_exists = False
        self.file_index = None
        self.file_by_uuid = None
        self.file_by_hash = None
        self.object_id = None
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

        self.object_id = doc.get('object_id')

        super(FileUploadEntity, self).parse(doc)

    def get_node_create(self, skip_node_lookup=False):
        """
        This is called for a POST operation.

        Return:
            psqlgraph.Node
        """
        # ################################################################
        # SignpostClient is used instead of IndexClient for the GDCAPI.
        # This means that the client doesn't have access to IndexClient's
        # methods, causing exceptions to occur.
        #
        # Temporary workaround until gdcapi uses indexd
        # ################################################################
        if self._config.get('USE_SIGNPOST', False):
            if self.entity_id:
                self.record_error(
                    'Cannot assign ID to file, these are system generated. ',
                    keys=['id'],
                    type=EntityErrors.INVALID_VALUE,
                )
            else:
                doc = self._create_index()
                self.entity_id = doc.did
                self.file_exists = True
        else:
            self._populate_files_from_index()
            self._set_node_and_file_ids()

        # call to super must happen after setting node and file ids here
        node = super(FileUploadEntity, self).get_node_create(
            skip_node_lookup=skip_node_lookup)
        node.acl = self.transaction.get_phsids()

        if self.use_object_id(self.entity_type):
            node._props['object_id'] = self.object_id

        return node

    def get_node_merge(self):
        """
        This is called for a PATCH operation and supports upsert. It will
        lookup an existing node or create one if it doesn't exist.

        Return:
            psqlgraph.Node:
        """
        # entity_id is set to the node_id here
        node = super(FileUploadEntity, self).get_node_merge()

        # verify that update is allowed
        if not self.is_updatable_file_node(node):
            self.record_error(
                ("This file is already in file_state '{}' and cannot be "
                 "updated. The raw data exists in the file storage "
                 "and modifying the Entity now is unsafe and may cause "
                 "problems for any processes or users consuming "
                 "this data.").format(node._props.get('file_state')),
                keys=['file_state'],
                type=EntityErrors.INVALID_PERMISSIONS,
            )

        # ################################################################
        # SignpostClient is used instead of IndexClient for the GDCAPI.
        # This means that the client doesn't have access to IndexClient's
        # methods, causing exceptions to occur.
        #
        # Temporary workaround until gdcapi uses indexd
        # ################################################################
        if self._config.get('USE_SIGNPOST', False):
            pass
        else:
            if (self.use_object_id(self.entity_type)
                and not self.object_id and 'object_id' in node._props):
                    self.object_id = node._props['object_id']

            self._populate_files_from_index()

            if not self.use_object_id(self.entity_type):
                # when object_id isn't used, we force 1:1 between indexed id
                # and node id.
                # NOTE: The call below populates record errors
                self._is_index_id_identical_to_node_id()
            else:
                if self.file_exists:
                    self._is_valid_index_for_file()

        return node

    def flush_to_session(self):
        """
        Depending on the role and status of the file in the index service,
        register or update an index. Then call parent class's
        flush_to_session.
        """

        # ################################################################
        # SignpostClient is used instead of IndexClient for the GDCAPI.
        # This means that the client doesn't have access to IndexClient's
        # methods, causing exceptions to occur.
        #
        # Temporary workaround until gdcapi uses indexd
        # ################################################################
        if self._config.get('USE_SIGNPOST', False):
            pass
        else:
            if not self.node:
                return

            role = self.action
            try:
                if role == 'create':
                    # Check if the category for the node is data_file or
                    # metadata_file, in which case, register a UUID and alias in
                    # the index service.
                    if not self.file_exists:
                        if (self._config.get('REQUIRE_FILE_INDEX_EXISTS', False)):
                            raise NoIndexForFileError(self.entity_id)
                        else:
                            self._register_index()

                elif role == 'update':
                    # Check if the category for the node is data_file or
                    # metadata_file, in which case, register a UUID and alias in
                    # the index service.
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
        Call the "signpost" (index client) for the transaction to register a
        new index record for this entity.
        """
        project_id = self.transaction.project_id
        submitter_id = self.node._props.get('submitter_id')
        hashes = {'md5': self.node._props.get('md5sum')}
        size = self.node._props.get('file_size')
        alias = "{}/{}".format(project_id, submitter_id)
        project = utils.lookup_project(
            self.transaction.db_driver,
            self.transaction.program,
            self.transaction.project
        )
        if utils.is_project_public(project):
            acl = ['*']
        else:
            acl = self.transaction.get_phsids()

        urls = []
        if self.urls:
            urls.extend(self.urls)

        # IndexClient
        doc = self._create_index(
            did=self.file_index,
            hashes=hashes,
            size=size,
            urls=urls,
            acl=acl
        )

        if self.use_object_id(self.entity_type):
            self.object_id = str(doc.did)
            self.node._props['object_id'] = self.object_id

        self._create_alias(
            record=alias, hashes=hashes, size=size, release='private'
        )

    def _update_index(self):
        """
        Call the "signpost" (index client) for the transaction to update an
        index record for this entity.
        """
        document = self.file_by_uuid or self.file_by_hash

        if self.urls:
            urls_to_add = [url for url in self.urls if url not in document.urls]
            if urls_to_add:
                document.urls.extend(urls_to_add)
                document.patch()

    @staticmethod
    def use_object_id(entity_type):
        """
        Check if the dictionary contains object_id for
        storing the GUID of the index record
        """
        ret = dictionary.schema.get(entity_type, {}).get('properties', {}).get('object_id')
        return ret

    @staticmethod
    def is_updatable_file_node(node):
        """
        Check that a node is a file that can be updated. True if:

        #. The node is a data_file
        #. It has a file_state in the list of states below

        Args:
            node (psqlgraph.Node):

        Return:
            bool: whether the node is an updatable file
        """
        is_updateable = True
        has_file_state = 'file_state' in node.__pg_properties__

        if has_file_state:
            allowed_states = [
                'registered',
                'uploading',
                'uploaded',
                'validating',
            ]
            file_state = node._props.get('file_state')
            if file_state and file_state not in allowed_states:
                is_updateable = False

        return is_updateable

    def _populate_files_from_index(self):
        """
        Populate information about file existence in index service.
        Will first check uuid then check by hash/size.

        If an object_id exists, that will be used to check indexd
        """
        uuid = self.entity_id
        if self.use_object_id(self.entity_type):
            if self.object_id:
                uuid = self.object_id
            else:
                uuid = None
        self.file_by_hash = self.get_file_from_index_by_hash()
        self.file_by_uuid = self.get_file_from_index_by_uuid(uuid)
        if self.file_by_uuid or self.file_by_hash:
            self.file_exists = True

    def _set_node_and_file_ids(self):
        """
        Set the node uuid and indexed file uuid accordingly based on
        information provided and whether or not the file exists in the
        index service.
        """
        if self.use_object_id(self.entity_type):
            # we are responsible for cleaning up entity id
            if not self.entity_id:
                self.entity_id = str(uuid.uuid4())

            if self.file_exists:
                if self.object_id:
                    self._is_valid_index_for_file()
                else:
                    self.object_id = getattr(self.file_by_hash, 'did', None)
            else:
                self.file_index = self.object_id

        else:
            if not self.file_exists:
                if self.entity_id:
                    # use entity_id for file creation
                    self.file_index = self.entity_id
                else:
                    # use same id for both node entity id and file index
                    self.entity_id = str(uuid.uuid4())
                    self.file_index = self.entity_id
            else:
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
                    if self._is_index_id_identical_to_node_id():
                        self.entity_id = file_by_hash_index

    def _is_valid_index_for_file(self):
        """
        Return whether or not uuid provided matches hash/size for file in index.

        Will first check for an indexed file with provided hash/size.
        Will then check for file with given uuid. Then will make sure
        those uuids match. record_errors will be set with any issues
        """
        is_valid = True

        entity_id = self.entity_id
        if self.use_object_id(self.entity_type):
            entity_id = self.object_id

        if (not self.file_by_hash or not self.file_by_uuid):
            error_message = (
                'Could not find exact file match in index for id: {} '
                'AND `hashes - size`: `{} - {}`. '
            ).format(
                entity_id,
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

        if is_valid and not self.use_object_id(self.entity_type):
            is_valid = self._is_index_id_identical_to_node_id()

        return is_valid

    def _is_index_id_identical_to_node_id(self):
        is_valid = True
        # if a single match exists in the graph, check to see if
        # file exists in index service
        nodes = lookup_node(
            self.transaction.db_driver,
            self.entity_type,
            self.entity_id,
            self.secondary_keys
        ).all()
        if len(nodes) == 1:
            if self.file_exists:
                file_by_uuid_index = getattr(self.file_by_uuid, 'did', None)
                file_by_hash_index = getattr(self.file_by_hash, 'did', None)
                if ((file_by_uuid_index != nodes[0].node_id) or
                        (file_by_hash_index != nodes[0].node_id)):
                    self.record_error(
                        'Graph ID and index file ID found in index service do not match, '
                        'which is currently not permitted. Graph ID: {}. '
                        'Index ID: {}. Index ID found using hash/size: {}.'
                        .format(nodes[0].node_id, file_by_hash_index, file_by_uuid_index),
                        type=EntityErrors.NOT_UNIQUE,
                    )
                    is_valid = False

        return is_valid

    def get_file_from_index_by_hash(self):
        """
        Return the record entity from "signpost" (index client)

        NOTE: Should only ever be called for data and metadata files.
        """
        document = None

        # ################################################################
        # SignpostClient is used instead of IndexClient for the GDCAPI.
        # This means that the client doesn't have access to IndexClient's
        # methods, causing exceptions to occur.
        #
        # Temporary workaround until gdcapi uses indexd
        # ################################################################
        if not self._config.get('USE_SIGNPOST', False):
            # Check if there is an existing record with this hash and size, i.e.
            # this node already has an index record.
            params = self._get_file_hashes_and_size()
            # document: indexclient.Document
            # if `document` exists, `document.did` is the UUID that is already
            # registered in indexd for this entity.
            if params:
                document = self.transaction.signpost.get_with_params(params)

        return document

    def get_file_from_index_by_uuid(self, uuid):
        """
        Return the record entity from 'signpost" (index client)

        NOTE: Should only ever be called for data and metadata files.
        """
        document = None

        if uuid:
            document = self.transaction.signpost.get(uuid)

        return document

    def _get_file_hashes_and_size(self):
        hashes = self._get_file_hashes()
        size = self._get_file_size()
        if hashes and size:
            return {'hashes': hashes, 'size': size}
        elif hashes:
            return {'hashes': hashes}
        elif size:
            return {'size': size}
        return None

    def _get_file_hashes(self):
        if self.doc.get('md5sum'):
            return {'md5': self.doc.get('md5sum')}
        return None

    def _get_file_size(self):
        if self.doc.get('file_size'):
            return self.doc.get('file_size')
        return None

    def _create_alias(self, **kwargs):
        return self.transaction.signpost.create_alias(**kwargs)

    def _create_index(self, **kwargs):
        return self.transaction.signpost.create(**kwargs)
