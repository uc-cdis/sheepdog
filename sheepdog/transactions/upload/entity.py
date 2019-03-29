"""
Classes and functions for upload.
"""

import uuid

import psqlgraph
import flask
import sqlalchemy
from psqlgraph.exc import ValidationError

from sheepdog import dictionary
from sheepdog import models
from sheepdog.auth import get_program_project_roles
from sheepdog.errors import InternalError
from sheepdog.globals import (
    REGEX_UUID,
    UNVERIFIED_PROGRAM_NAMES,
    UNVERIFIED_PROJECT_CODES,
)
from sheepdog.transactions.entity_base import EntityBase, EntityErrors
from sheepdog.utils import get_suggestion


def lookup_node(psql_driver, label, node_id=None, secondary_keys=None):
    """Return a query for nodes by id and secondary keys."""
    cls = psqlgraph.Node.get_subclass(label)
    query = psql_driver.nodes(cls)
    if node_id is None and not secondary_keys:
        return query.filter(sqlalchemy.sql.false())
    if node_id is not None:
        query = query.ids(node_id)
    if all(all(keys) for keys in secondary_keys):
        query = query.filter(cls._secondary_keys == secondary_keys)
    return query


class UploadEntity(EntityBase):
    """
    An `UploadEntity` is any biospecimen, file, admin, etc. entity to be owned by
    an :ref:``UploadTransaction`` to encapsulate validation and model creation.

    The lifespan of a UploadEntity should include:
       #. parse
       #. pre_validate
       #. instantiate
       #. post_validate

    These steps should call any validators specified by the data dictionary and
    create a node. After this, it should be flushed into a UploadTransaction's
    session.
    """

    DATA_FILE_CATEGORIES = ["data_file", "metadata_file", "index_file"]

    def __init__(self, transaction, config=None):
        """
        Args:
            transaction (UploadTransaction): the associated transaction
        """
        super(UploadEntity, self).__init__(transaction)
        self.doc = {}
        self.parents = {}
        self._secondary_keys = None
        self._config = config

    @property
    def secondary_keys(self):
        """Return the tuple of unique dicts for the node."""
        if self._secondary_keys is None:
            node = self.node or self.get_skeleton_node(self.entity_type, self.doc)
            if node:
                self._secondary_keys = node._secondary_keys
            else:
                self._secondary_keys = tuple()
        return self._secondary_keys

    @property
    def secondary_keys_dicts(self):
        """Return the list of unique tuples for the node."""
        node = self.node or self.get_skeleton_node(self.entity_type, self.doc)
        return [] if not node else node._secondary_keys_dicts

    @property
    def pg_secondary_keys(self):
        """Return the list of unique tuples for the node type"""

        node = self.node or self.get_skeleton_node(self.entity_type, self.doc)
        return [] if not node else node.__pg_secondary_keys

    def parse(self, doc):
        """
        Parse the given doc and set the instance values accordingly.

        Args:
            doc (dict): the json upload representation

        Return:
            None
        """
        if not isinstance(doc, dict):
            return self.record_error(
                "Entity document must be an object, not a {}".format(
                    doc.__class__.__name__
                ),
                type=EntityErrors.INVALID_VALUE,
            )

        self.doc = doc
        self._parse_type()
        if self.entity_type and self.is_valid:
            self._parse_id()

    def instantiate(self):
        """
        Create the graph node by populating necessary information within this
        instance. Will actually be added when flushed to the session.
        """
        if not self.is_valid:
            return

        # Check that there is an identifier
        if not (self.entity_id or self.secondary_keys):
            keys = [a for b in self.pg_secondary_keys for a in b]
            if not keys:
                return self.record_error(
                    (
                        "There are no unique keys defined on type {} except"
                        " for the official GDC id.  To upload this entity you"
                        " must add a UUID"
                    ).format(self.entity_type),
                    keys=["id"],
                    type=EntityErrors.MISSING_PROPERTY,
                )
            else:
                return self.record_error(
                    "Either an id or required unique fields ({}) required".format(
                        ", ".join(list(keys))
                    ),
                    keys=keys,
                    type=EntityErrors.MISSING_PROPERTY,
                )

        # Create entity
        if not self.entity_type:
            return

        if self.transaction.role == "create":
            self.node = self.get_node_create()
        elif self.transaction.role == "update":
            self.node = self.get_node_merge()
        else:
            self.record_error(
                "Unknown role '{}'".format(self.transaction.role),
                type=EntityErrors.INVALID_PERMISSIONS,
            )
            return

        # Stop if the node instantiation failed
        if not self.node:
            return

        self._set_node_properties()

    def flush_to_session(self):
        """
        Add graph node to session of the current transaction.
        """
        if not self.node:
            return

        role = self.action
        try:
            if role == "create":
                self.transaction.session.add(self.node)
            elif role == "update":
                self.node = self.transaction.session.merge(self.node)
            else:
                message = "Unknown role {}".format(role)
                self.logger.error(message)
                self.record_error(message, type=EntityErrors.INVALID_PERMISSIONS)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.exception(e)
            self.record_error(str(e))

    def _parse_id(self):
        """
        Generate and record the entity id.

        :returns: None

        """
        self.entity_id = self.doc.get("id")
        not_uuid = (
            self.transaction.role == "create"
            and self.entity_id
            and not REGEX_UUID.match(self.entity_id)
        )
        if not_uuid:
            self.record_error(
                "Cannot create entity with custom id that is not a UUID.",
                keys=["id"],
                type=EntityErrors.INVALID_VALUE,
            )

    def _parse_type(self):
        """
        Parse and record the entity type. This type will be used to look up
        what node class to instantiate.

        Return:
            None
        """
        self.entity_type = self.doc.get("type")
        if self.entity_type is None:
            return self.record_error(
                "missing 'type'", keys=["type"], type=EntityErrors.INVALID_TYPE
            )
        self._validate_type()

    def get_node_create(self, skip_node_lookup=False):
        """
        This is called for a POST operation.

        Check that:

        #. The user isn't supplying an id
        #. If not `skip_node_lookup`, the node does not already exist

        Return:
            psqlgraph.Node
        """
        # Check user permissions for updating nodes
        roles = self.get_user_roles()
        if "create" not in roles:
            return self.record_error(
                "You do not have create permission for project {} only {}".format(
                    self.transaction.project_id, roles
                ),
                type=EntityErrors.INVALID_PERMISSIONS,
            )

        # Check to see if it's registered in dbGaP is is an exception
        # to the rule
        if self.entity_type == "case":
            submitter_id = self.doc.get("submitter_id")

            allowed = True
            try:
                if self._config.get("USE_DBGAP", False):
                    allowed = self.is_case_creation_allowed(submitter_id)

            except InternalError as e:
                return self.record_error(
                    "Unable to validate case against dbGaP. {}".format(str(e)),
                    keys=["submitter_id"],
                    type=EntityErrors.NOT_FOUND,
                )

            else:
                if not allowed:
                    return self.record_error(
                        "Case submitter_id '{}' not found in dbGaP.".format(
                            submitter_id
                        ),
                        keys=["submitter_id"],
                        type=EntityErrors.NOT_FOUND,
                    )

        if not self.entity_id:
            self.entity_id = str(uuid.uuid4())

        # Assert that the node doesn't already exist
        if not skip_node_lookup:
            nodes = lookup_node(
                self.transaction.db_driver,
                self.entity_type,
                self.entity_id,
                self.secondary_keys,
            )
            if nodes.count():
                return self.record_error(
                    "Cannot create entity that already exists. "
                    "Try updating entity (PUT instead of POST)",
                    keys=["id"],
                    type=EntityErrors.NOT_UNIQUE,
                )

        # Fill in default system property values
        for key, val in self.get_system_property_defaults().iteritems():
            if self.doc.get(key, None) is None:
                self.doc[key] = val

        # Create the node and populate its properties
        cls = psqlgraph.Node.get_subclass(self.entity_type)
        self.logger.debug("Creating new {}".format(cls.__name__))
        node = cls(self.entity_id)

        self.action = "create"

        return node

    def get_node_merge(self):
        """
        This is called for a PATCH operation and supports upsert. It will
        lookup an existing node or create one if it doesn't exist.

        Return:
            Optional[psqlgraph.Node]
        """
        # Expect one existing node matching the secondary keys.
        try:
            node = lookup_node(
                self.transaction.db_driver,
                self.entity_type,
                self.entity_id,
                self.secondary_keys,
            ).one()
        except sqlalchemy.orm.exc.NoResultFound:
            return self.get_node_create()
        except sqlalchemy.orm.exc.MultipleResultsFound:
            self.record_error(
                "Entity is not unique, multiple entities found with {}".format(
                    self.secondary_keys
                ),
                type=EntityErrors.NOT_UNIQUE,
            )
            return None

        # Check user permissions for updating nodes
        if "update" not in self.get_user_roles():
            self.record_error(
                "You do not have update permission for project {}".format(
                    self.transaction.project_id
                ),
                type=EntityErrors.INVALID_PERMISSIONS,
            )
            return None

        self.old_props = {k: v for k, v in node.props.iteritems()}

        if node.label != self.entity_type:
            self.record_error(
                "Existing {} entity found with type different from {}".format(
                    node.label, self.entity_type
                ),
                type=EntityErrors.NOT_UNIQUE,
            )
            return None

        # Verify that the node is in the correct project
        if not self._verify_node_project_id(node):
            self.record_error(
                "Entity is owned by project {}, not {}".format(
                    node.project_id, self.transaction.project_id
                ),
                type=EntityErrors.INVALID_PERMISSIONS,
            )
        self._merge_doc_links(node)

        if self.entity_id and node.node_id != self.entity_id:
            self.record_error(
                "Existing {} entity found with id different from {}".format(
                    node, self.entity_id
                ),
                type=EntityErrors.NOT_UNIQUE,
            )
            return None

        # Since we are updating the node, we have to set its state to
        # ``validated``.  This means that this version of the node has
        # not been submitted and will not be displayed on the portal.
        # The node is now essential a draft.
        if node.state is None:
            node.state = "validated"

        # Fill in default system property values
        for key in self.get_system_property_defaults():
            self.logger.debug(
                "{}: setting system prop self.doc['{}'] to '{}'".format(
                    node, key, node._props.get(key)
                )
            )
            if self.doc.get(key) and self.doc.get(key) != node._props.get(key):
                msg = (
                    "Property '{}' ({}) is a system property and will be" " ignored."
                ).format(key, self.doc.get(key))
                self.record_warning(msg, keys=[key], type=EntityErrors.INVALID_PROPERTY)
            self.doc[key] = node._props.get(key)

        self.action = "update"
        self.entity_id = node.node_id

        return node

    def _merge_doc_links(self, node):
        """
        For all links that exist in the database, add them to the document if
        the user didn't supply them.
        """
        for name in node._pg_links:

            doc_sk, doc_ids = set(), set()
            doc_links = self.doc.get(name, [])

            # Munge to a list
            if isinstance(doc_links, dict):
                doc_links = [doc_links]
            self.doc[name] = doc_links

            # Get set of node ids in the link document
            map(doc_ids.add, [l.get("id") for l in doc_links if l.get("id")])

            # Get set of secondary_keys in the link document
            for link in doc_links:
                target_label = node._pg_links.get(name, {}).get("dst_type", None).label
                target = self.get_skeleton_node(target_label, link)
                if target:
                    doc_sk.add(target._secondary_keys)

            # Add links that are in the database, but not the JSON doc
            for n in getattr(node, name):
                node_in_doc = n._secondary_keys in doc_sk or n.node_id in doc_ids
                if not node_in_doc:
                    self.doc[name].append({"id": n.node_id})

            # Remove empty link list
            if not self.doc[name]:
                self.doc.pop(name)

    def _remove_empty_values(self, doc):
        for key in doc.keys():
            value = doc[key]

            if isinstance(value, dict):
                self._remove_empty_values(value)
            elif hasattr(value, "__iter__"):
                value = [
                    sub_doc
                    for sub_doc in map(self._remove_empty_values, value)
                    if sub_doc
                ]

            is_removed = value == {} or value == [] or value is None
            if is_removed:
                doc.pop(key)

        return doc

    def _set_node_properties(self):
        """
        Take the key, values from the dictionary (minus system keys) and set
        the value on the instances node, recording any errors.
        """
        self.logger.debug("Setting properties on {}".format(self.node))
        entry = dictionary.schema.get(self.node.label, {})
        systemProperties = set(entry.get("systemProperties", []))

        special_keys = ["type", "id", "created_datetime", "updated_datetime"]
        pg_props = self.node.get_pg_properties()
        prop_keys = pg_props.keys() + self.node._pg_links.keys() + special_keys
        self.node.project_id = self.transaction.project_id
        default_props = self.get_system_property_defaults()

        # Set properties
        for key, val in self.doc.iteritems():

            # Does this key exist?
            if key not in prop_keys:
                msg = (
                    "Key '{}' is not a valid property for type '{}'.{}".format(
                        key, self.entity_type, get_suggestion(key, prop_keys)
                    ),
                )
                self.record_error(msg, keys=[key], type=EntityErrors.INVALID_PROPERTY)

            # Skip type and id
            elif key in special_keys:
                pass

            # If key is a link, skip for now
            elif key in self.node._pg_links.keys():
                pass

            # Is it a system property?
            elif key in systemProperties:

                # If the property isn't set on the node, set the default
                if self.node._props.get(key, None) is None:
                    default = default_props.get(key, None)
                    self.logger.debug(
                        "{}: setting null system property '{}' to {}".format(
                            self.node, key, default
                        )
                    )
                    self.node._props[key] = default

                elif self.node._props.get(key) != val:
                    self.record_error(
                        (
                            "Key '{}' is a system property and cannot be updated "
                            "from '{}' to '{}'"
                        ).format(key, self.node._props.get(key), val),
                        keys=[key],
                        type=EntityErrors.INVALID_PERMISSIONS,
                    )

            # Otherwise, set the value
            else:
                try:
                    self.node._props[key] = val
                except Exception as e:  # pylint: disable=broad-except
                    self.record_error(
                        "Invalid property ({}): {}".format(key, str(e)),
                        keys=[key],
                        type=EntityErrors.INVALID_PROPERTY,
                    )

    def _validate_type(self):
        """
        Assert that the requested type is valid and uploadable via the
        project endpoint.
        """
        cls = psqlgraph.Node.get_subclass(self.entity_type)

        if not cls:
            return self.record_error(
                "Invalid entity type: {}.{}".format(
                    self.entity_type,
                    get_suggestion(
                        self.entity_type,
                        [n.label for n in psqlgraph.Node.get_subclasses()],
                    ),
                ),
                keys=["type"],
                type=EntityErrors.INVALID_TYPE,
            )

        if "project_id" not in cls.get_pg_properties():
            msg = "{} is not an entity that can be upload via the project" " endpoint."
            self.record_error(
                msg.format(cls.label), keys=["id"], type=EntityErrors.INVALID_TYPE
            )

    def _verify_node_project_id(self, node):
        """
        Check if a node is in the project that this entity belongs to.

        Args:
            node (psqlgraph.Node)

        Return:
            bool: if existing node belongs to the correct project
        """
        return node.project_id == self.transaction.project_id

    def get_skeleton_node(self, label, properties, project_id=None):
        """
        Return a node with just the properties set
        """
        # Get node class
        cls = psqlgraph.Node.get_subclass(label)
        if not cls:
            return None
        node = cls()

        # Set project_id.
        project_id_is_unset = (
            "project_id" in node.__pg_properties__ and "project_id" not in properties
        )
        if project_id_is_unset:
            project_id = project_id or self.transaction.project_id
            node["project_id"] = project_id

        # Set given properties.
        for key, val in properties.iteritems():
            if key in node.__pg_properties__:
                try:
                    node[key] = val
                except ValidationError:
                    # pass for now, this will be noted later
                    pass

        return node

    def get_system_property_defaults(self):
        """
        Return a dictionary with systemProperty values populated by defaults.
        """
        entry = dictionary.schema.get(self.entity_type, {})
        system_properties = entry.get("systemProperties", {})
        doc = {}
        ignored_system_property_keys = {
            "id",
            "type",
            "created_datetime",
            "updated_datetime",
            "project_id",
        }

        for key in system_properties:
            if key in ignored_system_property_keys:
                continue
            node_props = psqlgraph.Node.get_subclass(self.entity_type).__pg_properties__
            if key not in node_props:
                self.logger.error(
                    "'{}' has systemProperty '{}' that is not a property".format(
                        type(self.node), key
                    )
                )
                continue
            # If the property doesn't have a value, and a default value
            # is provided in dictionary, set the default to the key
            prop = entry.get("properties", {}).get(key, {})
            if "default" in prop:
                doc[key] = prop["default"]
        return doc

    def get_user_roles(self):
        return get_program_project_roles(*self.transaction.project_id.split("-", 1))

    def is_case_creation_allowed(self, case_id):
        """
        Check if case creation is allowed:

        #. Does the case exist in dbGaP?
        #. Is the case in a predefined list of cases to allow?
        #. Is the owning project in a predefined list of projects?
        """
        program, project = self.transaction.project_id.split("-", 1)
        if program in UNVERIFIED_PROGRAM_NAMES:
            return True
        elif project in UNVERIFIED_PROJECT_CODES:
            return True
        else:
            return self.transaction.dbgap_x_referencer.case_exists(
                program, project, self.doc.get("submitter_id")
            )

    def set_association_proxies(self):
        """
        Set all links on the actual node instance.
        """
        if not self.node:
            return

        for name in self.node._pg_links:

            # Grab the link documents, e.g. sample.portions
            links = self.doc.get(name)
            if not links:
                continue

            # Munge to a list for ease of use
            if isinstance(links, dict):
                links = [links]

            for link in links:
                # Get target information
                target_id = link.get("id")
                target_label = (
                    self.node._pg_links.get(name, {}).get("dst_type", None).label
                )
                target = self.get_skeleton_node(target_label, link)

                # Query for targets
                nodes = lookup_node(
                    self.transaction.db_driver,
                    self.node._pg_links[name]["dst_type"].label,
                    node_id=target_id,
                    secondary_keys=target._secondary_keys,
                ).all()

                # Verify any link to projects is in the correct project
                if self.node._pg_links[name]["dst_type"] == models.Project:
                    for node in nodes:
                        if node.code != self.transaction.project:
                            self.record_error(
                                "Cannot link entity to project"
                                " {} under {} endpoint".format(
                                    node.code, self.transaction.project
                                ),
                                type=EntityErrors.INVALID_PERMISSIONS,
                            )

                # Check for duplicates
                if len(nodes) > 1:
                    self.record_error(
                        "More than one link destination found for {}".format(name),
                        keys=[name],
                        type=EntityErrors.INVALID_LINK,
                    )
                    continue

                # Check for missing links
                elif len(nodes) == 0:
                    msg = "No link destination found for {}".format(name)
                    if target_id:
                        msg += ", id='{}'".format(target_id)

                    msg += ", unique_keys='{}'".format(target._secondary_keys_dicts)
                    # NOTE: this file is not using "coding: utf-8"; it it were,
                    # this would have to use a json.loads(json.dumps(...))
                    # cycle to remove the unicode articact 'u' in front of all
                    # the keys.

                    self.record_error(msg, keys=[name], type=EntityErrors.INVALID_LINK)
                    continue

                # Finally, add the target to the association proxy list
                for n in nodes:
                    disallowed = (
                        "project_id" in n.props
                        and n.project_id != self.transaction.project_id
                    )
                    if disallowed:
                        self.record_error(
                            "Relationship to {} {} in project {} not allowed".format(
                                n.label, n.node_id, n.project_id
                            ),
                            type=EntityErrors.INVALID_LINK,
                        )
                    if n not in getattr(self.node, name):
                        getattr(self.node, name).append(n)

    def specify_errors(self):
        """
        TODO
        """
        for error in self.errors:
            if error["type"] == EntityErrors.UNCATEGORIZED:
                message = error["message"]
                if "is not of type" in message:
                    error["type"] = EntityErrors.INVALID_VALUE
                elif "is a required property" in message:
                    error["type"] = EntityErrors.MISSING_PROPERTY
                elif self.entity_type and error.get("keys"):
                    cls = psqlgraph.Node.get_subclass(self.entity_type)
                    if cls and error["keys"][0] in cls._pg_edges:
                        error["type"] = EntityErrors.INVALID_LINK
