"""
Subclasses for UploadEntity that handle different types of
uploaded entites.
"""

import uuid
import requests
import json

import flask

from sheepdog.auth import current_token
from sheepdog.errors import NoIndexForFileError, UserError

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
        3) The hash/file_size combo provided should be unique in the index
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
        self.should_update_acl_and_authz = False

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
        entity_urls = doc.get("urls")
        if entity_urls:
            self.urls = entity_urls.strip().split(",")
            # remove from the doc since we don't want to validate it
            doc.pop("urls")

        self.object_id = doc.get("object_id")

        super(FileUploadEntity, self).parse(doc)

    def get_node_create(self, skip_node_lookup=False):
        """
        This is called for a POST operation.

        Return:
            psqlgraph.Node
        """
        self._populate_files_from_index()

        # file already indexed and object_id provided: data upload flow
        if self.use_object_id(self.entity_type) and self.object_id and self.file_exists:
            if (
                self._is_valid_hash_size_for_file()
                and not getattr(self.file_by_uuid, "acl", None)
                and not getattr(self.file_by_uuid, "authz", None)
            ):
                self.should_update_acl_and_authz = True
        else:
            self._set_node_and_file_ids()

        # call to super must happen after setting node and file ids here
        node = super(FileUploadEntity, self).get_node_create(
            skip_node_lookup=skip_node_lookup
        )
        node.acl = self.transaction.get_phsids()

        if self.use_object_id(self.entity_type):
            node._props["object_id"] = self.object_id

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
        if not node:
            # `get_node_merge` must have errored in `UploadEntity`, which should
            # record the error, so just pass the `None` return upwards
            return None

        # verify that update is allowed
        if not self.is_updatable_file_node(node):
            self.record_error(
                (
                    "This file is already in file_state '{}' and cannot be "
                    "updated. The raw data exists in the file storage "
                    "and modifying the Entity now is unsafe and may cause "
                    "problems for any processes or users consuming "
                    "this data."
                ).format(node._props.get("file_state")),
                keys=["file_state"],
                type=EntityErrors.INVALID_PERMISSIONS,
            )

        if (
            self.use_object_id(self.entity_type)
            and not self.object_id
            and "object_id" in node._props
        ):
            self.object_id = node._props["object_id"]

        self._populate_files_from_index()

        if not self.use_object_id(self.entity_type):
            # when object_id isn't used, we force 1:1 between indexed id
            # and node id.
            # NOTE: The call below populates record errors
            self._is_index_id_identical_to_node_id()
        else:
            if self.file_exists and not self.object_id:
                self._is_valid_index_for_file()

        return node

    def flush_to_session(self):
        """
        Depending on the role and status of the file in the index service,
        register or update an index. Then call parent class's
        flush_to_session.
        """

        if not self.node:
            return

        role = self.action
        try:
            if role == "create":
                # data upload flow: update the blank record in indexd
                if self.should_update_acl_and_authz:
                    self._update_acl_uploader_for_file()

                    # Temporary fix to update authz field in index record
                    # in the data upload flow case,
                    # while we don't have a way to do it properly (i.e. with permissions checks)
                    document = self.file_by_uuid or self.file_by_hash
                    namespace = flask.current_app.config.get(
                        "AUTH_NAMESPACE", ""
                    ).rstrip("/")
                    authz = [
                        "{}/programs/{}/projects/{}".format(
                            namespace,
                            self.transaction.program,
                            self.transaction.project,
                        )
                    ]
                    use_consent_codes = (
                        dictionary.schema.get(self.entity_type, {})
                        .get("properties", {})
                        .get("consent_codes")
                    )
                    if use_consent_codes:
                        consent_codes = self.node._props.get("consent_codes")
                        if consent_codes:
                            authz.extend("/consents/" + code for code in consent_codes)
                    document.authz = authz
                    document.patch()
                    # End temporary fix

                # Check if the category for the node is data_file or
                # metadata_file, in which case, register a UUID and alias in
                # the index service.
                elif not self.file_exists:
                    if self._config.get("REQUIRE_FILE_INDEX_EXISTS", False):
                        raise NoIndexForFileError(self.entity_id)
                    else:
                        self._register_index()

            elif role == "update":
                # Check if the category for the node is data_file or
                # metadata_file, in which case, register a UUID and alias in
                # the index service.
                if self.file_exists:
                    self._update_index()

            else:
                message = "Unknown role {}".format(role)
                self.logger.error(message)
                self.record_error(message, type=EntityErrors.INVALID_PERMISSIONS)
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
        submitter_id = self.node._props.get("submitter_id")
        hashes = {"md5": self.node._props.get("md5sum")}
        size = self.node._props.get("file_size")
        alias = "{}/{}".format(project_id, submitter_id)
        project = utils.lookup_project(
            self.transaction.db_driver,
            self.transaction.program,
            self.transaction.project,
        )
        if utils.is_project_public(project):
            acl = ["*"]
        else:
            acl = self.transaction.get_phsids()

        urls = []
        if self.urls:
            urls.extend(self.urls)

        namespace = flask.current_app.config.get("AUTH_NAMESPACE", "").rstrip("/")
        authz = [
            "{}/programs/{}/projects/{}".format(
                namespace, self.transaction.program, self.transaction.project
            )
        ]
        use_consent_codes = (
            dictionary.schema.get(self.entity_type, {})
            .get("properties", {})
            .get("consent_codes")
        )
        if use_consent_codes:
            consent_codes = self.node._props.get("consent_codes")
            if consent_codes:
                authz.extend("/consents/" + code for code in consent_codes)

        # IndexClient
        doc = self._create_index(
            did=self.file_index,
            hashes=hashes,
            size=size,
            urls=urls,
            acl=acl,
            authz=authz,
        )

        if self.use_object_id(self.entity_type):
            self.object_id = str(doc.did)
            self.node._props["object_id"] = self.object_id

        self._create_alias(alias, doc.did)

    def _update_index(self):
        """
        Call the index client for the transaction to update an
        index record for this entity.
        """
        document = self.file_by_uuid or self.file_by_hash

        if self.urls != []:
            document.urls = self.urls
            # remove url metadata for deleted urls if that happens
            document.urls_metadata = {
                k: v for (k, v) in document.urls_metadata.items() if k in document.urls
            }
            document.patch()

    @staticmethod
    def use_object_id(entity_type):
        """
        Check if the dictionary contains object_id for
        storing the GUID of the index record
        """
        ret = (
            dictionary.schema.get(entity_type, {})
            .get("properties", {})
            .get("object_id")
        )
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
        if not node:
            return False
        if "file_state" in node.__pg_properties__:
            allowed_states = ["registered", "uploading", "uploaded", "validating"]
            file_state = node._props.get("file_state")
            if file_state and file_state not in allowed_states:
                return False
        return True

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
                if not self.object_id:
                    self.object_id = getattr(self.file_by_hash, "did", None)
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
                    file_by_hash_index = getattr(self.file_by_hash, "did", None)

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

        if not self.file_by_hash or not self.file_by_uuid:
            error_message = (
                "Could not find exact file match in index for id: {} "
                "AND `hashes - size`: `{} - {}`. "
            ).format(
                entity_id, str(self._get_file_hashes()), str(self._get_file_size())
            )

            if self.file_by_hash:
                error_message += "A file was found matching `hash / size` but NOT id."
            elif self.file_by_uuid:
                error_message += "A file was found matching id but NOT `hash / size`."
            else:
                # keep generic error message since both didn't result in a match
                pass

            self.record_error(error_message, type=EntityErrors.INVALID_VALUE)
            is_valid = False

        if (
            self.file_by_hash
            and self.file_by_uuid
            and self.file_by_hash.did != self.file_by_uuid.did
        ):
            # both exist but dids are different
            # FIXME: error should be handled different/removed
            #        when we support updating indexed files
            self.record_error(
                "Provided id for indexed file {} does not match the id "
                "for the file discovered in the index by hash/size ("
                "id: {}). Updating a previous index with new file "
                "is currently NOT SUPPORTED.".format(
                    self.file_by_uuid.did, self.file_by_hash.did
                ),
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
            self.secondary_keys,
        ).all()
        if len(nodes) == 1:
            if self.file_exists:
                file_by_uuid_index = getattr(self.file_by_uuid, "did", None)
                file_by_hash_index = getattr(self.file_by_hash, "did", None)
                if (file_by_uuid_index != nodes[0].node_id) or (
                    file_by_hash_index != nodes[0].node_id
                ):
                    self.record_error(
                        "Graph ID and index file ID found in index service do not match, "
                        "which is currently not permitted. Graph ID: {}. "
                        "Index ID: {}. Index ID found using hash/size: {}.".format(
                            nodes[0].node_id, file_by_hash_index, file_by_uuid_index
                        ),
                        type=EntityErrors.NOT_UNIQUE,
                    )
                    is_valid = False

        return is_valid

    def _is_valid_hash_size_for_file(self):
        """
        Return whether or not the provided hash and size match those of the existing file in indexd.

        Should only be called when the file already exists in indexd and the object_id is provided (data upload flow).
        """
        if (
            not self.use_object_id(self.entity_type)
            or not self.object_id
            or not self.file_exists
        ):
            self.record_error(
                "The object_id of an indexed file must be provided.",
                type=EntityErrors.INVALID_VALUE,
            )
            return False

        entity_id = self.object_id

        # check that the file exists in indexd
        if not self.file_by_uuid:
            self.record_error(
                "Provided object_id {} does not match any indexed file.".format(
                    entity_id
                ),
                type=EntityErrors.INVALID_VALUE,
            )
            return False

        file_hashes = self.file_by_uuid.hashes
        file_size = self.file_by_uuid.size

        # empty hash and size mean the file is not ready for metadata submission yet
        if not file_hashes or not file_size:
            error_message = "Indexed file of id {} is not ready for metadata submission yet (no hashes and size).".format(
                entity_id
            )
            self.record_error(error_message, type=EntityErrors.INVALID_VALUE)
            return False

        # check that the provided hash/size match those of the file in indexd
        # submitted hashes have to be a subset of the indexd ones
        hashes_match = all(
            item in file_hashes.items() for item in self._get_file_hashes().items()
        )
        sizes_match = self._get_file_size() == file_size

        if not (hashes_match and sizes_match):
            error_message = "Provided hash ({}) and size ({}) do not match those of indexed file of id {}.".format(
                self._get_file_hashes(), self._get_file_size(), entity_id
            )
            self.record_error(error_message, type=EntityErrors.INVALID_VALUE)
            return False

        return True

    def _update_acl_uploader_for_file(self):
        """
        Update acl and uploader fields in indexd.

        Should only be called when the file already exists in indexd and the object_id is provided (data upload flow).
        """
        if (
            not self.use_object_id(self.entity_type)
            or not self.object_id
            or not self.file_exists
        ):
            self.record_error(
                "The object_id of an indexed file must be provided.",
                type=EntityErrors.INVALID_VALUE,
            )
            return

        file_uploader = self.file_by_uuid.uploader
        # if indexd uploader is empty, the file already belongs to a project.
        # do not update acl: other projects will just reference this file
        if not file_uploader:
            return

        # the current uploader must be the file uploader, and acl must be empty
        current_uploader = current_token["context"]["user"]["name"]
        file_acl = self.file_by_uuid.acl
        if not current_uploader == file_uploader or file_acl:
            self.record_error(
                "Failed to update acl and uploader fields in indexd: current uploader ({}) is not original file uploader ({}) and/or acl ({}) is not empty.".format(
                    current_uploader, file_uploader, file_acl
                ),
                type=EntityErrors.INVALID_VALUE,
            )
            return

        # update acl and uploader fields in indexd
        data = json.dumps({"acl": self.transaction.get_phsids(), "uploader": None})
        try:
            # This must be done via _put and _load as opposed to using document.patch()
            # because the uploader field is (correctly) not in indexclient's UPDATABLE_ATTRS
            self.transaction.index_client._put(
                "index",
                self.object_id,
                headers={"content-type": "application/json"},
                data=data,
                params={"rev": self.file_by_uuid.rev},
                auth=self.transaction.index_client.auth,
            )
            self.file_by_uuid._load()  # to sync new rev from server
        except requests.HTTPError as e:
            self.record_error(
                "Failed to update acl and uploader fields in indexd: {}".format(e)
            )

    def get_file_from_index_by_hash(self):
        """
        Return the record entity from index client

        NOTE: Should only ever be called for data and metadata files.
        """
        document = None

        # Check if there is an existing record with this hash and size, i.e.
        # this node already has an index record.
        params = self._get_file_hashes_and_size()
        # document: indexclient.Document
        # if `document` exists, `document.did` is the UUID that is already
        # registered in indexd for this entity.
        if params:
            try:
                document = self.transaction.index_client.get_with_params(params)
            except requests.HTTPError as e:
                raise UserError(
                    code=e.response.status_code,
                    message="Fail to register the data node in indexd. Detail {}".format(
                        e
                    ),
                )

        return document

    def get_file_from_index_by_uuid(self, uuid):
        """
        Return the record entity from index client

        NOTE: Should only ever be called for data and metadata files.
        """
        document = None

        if uuid:
            document = self.transaction.index_client.get(uuid)

        return document

    def _get_file_hashes_and_size(self):
        hashes = self._get_file_hashes()
        size = self._get_file_size()
        if hashes and size:
            return {"hashes": hashes, "size": size}
        elif hashes:
            return {"hashes": hashes}
        elif size:
            return {"size": size}
        return None

    def _get_file_hashes(self):
        if self.doc.get("md5sum"):
            return {"md5": self.doc.get("md5sum")}
        return None

    def _get_file_size(self):
        if self.doc.get("file_size"):
            return self.doc.get("file_size")
        return None

    def _create_alias(self, alias, did):
        return self.transaction.index_client.add_alias_for_did(alias, did)

    def _create_index(self, **kwargs):
        return self.transaction.index_client.create(**kwargs)
