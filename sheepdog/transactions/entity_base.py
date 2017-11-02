"""
TODO
"""

import abc

import psqlgraph

from sheepdog.globals import (
    case_cache_enabled,
)


class EntityErrors(object):
    """Enum of possible entity error classifications."""
    INVALID_LINK = "INVALID_LINK"
    INVALID_NUMERIC = "INVALID_NUMERIC"
    INVALID_PERMISSIONS = "INVALID_PERMISSIONS"
    INVALID_PROPERTY = "INVALID_PROPERTY"
    INVALID_TYPE = "INVALID_TYPE"
    INVALID_VALUE = "INVALID_VALUE"
    MISSING_PROPERTY = "MISSING_PROPERTY"
    NOT_UNIQUE = "NOT_UNIQUE"
    NOT_FOUND = "NOT_FOUND"
    UNCATEGORIZED = "ERROR"


class EntityBase(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, transaction, node=None):
        self.transaction = transaction
        self.logger = self.transaction.logger
        self.node = node
        self.errors = []
        self.warnings = []
        self.action = None
        self.old_props = {}
        if self.node:
            self.entity_id = node.node_id
            self.entity_type = node.label
        else:
            self.entity_id = None
            self.entity_type = None

    def __repr__(self):
        return '<{}{}>'.format(self.__class__.__name__, self.node or '<None>')

    @abc.abstractproperty
    def pg_secondary_keys(self):
        """Return the list of unique tuples for the node type"""

    @abc.abstractproperty
    def secondary_keys(self):
        """Return the list of unique dicts for the node"""

    @abc.abstractproperty
    def secondary_keys_dicts(self):
        """Return the list of unique tuples for the node"""

    @property
    def base_json(self):
        """Returns a json object representation.  This property is intended
        to be used to produce all server response payloads.

        Subclasses sould call and add to this base.

        """

        return {
            'valid': self.is_valid,
            'action': self.action,
            'type': self.entity_type,
            'id': self.entity_id,
            'related_cases': self.related_cases,
            'errors': self.errors,
            'warnings': self.warnings,
            'unique_keys': self.secondary_keys_dicts,
        }

    @property
    def is_valid(self):
        """
        Return bolean of whether this entity has no recorded errors.
        """
        return not self.errors

    @property
    def json(self):
        """Alias ``base_json``."""
        return self.base_json

    @property
    def node_exists(self):
        """Check that the node exists in the database."""
        if not self.node:
            return False
        count = (
            self.transaction.db_driver.nodes(self.node.__class__)
            .ids(self.node.node_id)
            .count()
        )
        return count > 0

    @property
    def related_cases(self):
        """Gets related cases from shortcut edge

        """
        if not case_cache_enabled() or not self.node or not self.entity_type or not self.node.label:
            return []
        self.transaction.session.flush()

        def make_id(case):
            return {'id': case.node_id, 'submitter_id': case.submitter_id}

        return map(make_id, self.node._related_cases_from_cache)

    def get_links(self, node):
        """Return the possible links to submittable entities given a node."""

        # TODO: send help

        if not node.label:
            return []
        cls = node.__class__

        return [
            (e.__src_dst_assoc__,
                psqlgraph.Node.get_subclass_named(e.__dst_class__).label)
            for e in psqlgraph.Edge._get_edges_with_src(cls.__name__)
            if hasattr(psqlgraph.Node.get_subclass_named(e.__dst_class__),
                        'project_id')
        ] + [
            (e.__dst_src_assoc__,
                psqlgraph.Node.get_subclass_named(e.__src_class__).label)
            for e in psqlgraph.Edge._get_edges_with_dst(cls.__name__)
            if hasattr(psqlgraph.Node.get_subclass_named(e.__src_class__),
                        'project_id')
        ]

    def set_old_props(self):
        """
        Snapshot the properties as they are now to put in transaction log.
        """
        self.old_props = {k: v for k, v in self.node.props.iteritems()}

    def record_error(self, message, keys=None, type=None, **kwargs):
        """
        Record an error message.

        Args:
            message (str): message explaining what went wrong
            keys (list): what keys in the JSON were invalid

        Return:
            None
        """
        keys = list(keys) if keys is not None else []
        self.errors.append(dict(
            message=message,
            keys=keys,
            type=type or EntityErrors.UNCATEGORIZED,
            **kwargs
        ))

    def record_warning(self, message, keys=None, **kwargs):
        """
        Record a warning message.

        Args:
            message (str): message explaining what went wrong
            keys (list): what keys in the JSON were invalid

        Return:
            None
        """
        keys = list(keys) if keys is not None else []
        self.warnings.append(dict(message=message, keys=keys, **kwargs))
