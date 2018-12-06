# pylint: disable=protected-access
# pylint: disable=unsubscriptable-object
# pylint: disable=unsupported-membership-test

from collections import defaultdict
import copy
import csv
import hashlib
import json
import StringIO
import tarfile
import time

from cdislogging import get_logger
import flask
import psqlgraph

from sheepdog import dictionary
from sheepdog.errors import (
    InternalError,
    NotFoundError,
    UnsupportedError,
    UserError,
)
from sheepdog.globals import (
    DELIMITERS,
    SUPPORTED_FORMATS,
)


log = get_logger(__name__)

TEMPLATE_NAME = 'submission_templates.tar.gz'

# This is the list of node categories which cannot be exported using the export
# endpoint, which will cover unsupported types like `root` and `_all`.
UNSUPPORTED_EXPORT_NODE_CATEGORIES = [
    'internal',
]


def get_node_category(node_type):
    """
    Get the category for the given node type specified

    Args:
        node_type (str): the type of node

    Returns:
        str: node category
    """
    cls = psqlgraph.Node.get_subclass(node_type)
    return cls._dictionary.get('category')


def parse_ids(ids):
    """
    Parse a list of ids from `ids` of unknown type.

    Args:
        ids: valid types are string, unicode, and list of strings

    Return:
        list: ids from `ids` of unknown type

    Raises:
        UserError: if any ids are invalid
    """
    if not ids:
        raise UserError('Please provide valid ids')

    if isinstance(ids, (str, unicode)):
        ids = ids.split(',')
    elif not isinstance(ids, list):
        raise UserError('Invalid list of ids: {}'.format(ids))

    # Assert that all entries in list are string or unicode
    if not all(isinstance(id_, (str, unicode)) for id_ in ids):
        raise UserError('Ids must be strings: {}'.format(ids))

    return ids


def get_link_props(props):
    """Return subset of iterable props that are links"""
    return [val for val in props if '.' in val]


def get_non_link_props(props):
    """Return subset of iterable props that are not links."""
    return [val for val in props if '.' not in val]


def get_link_name(key, number):
    """Return formatted link name over enumerated links."""
    return '{}#{}'.format(key, str(number+1))


def get_link_titles(entities, template):
    """Return a list of link tities for given entities and template."""
    titles = []

    for key in get_link_props(template):
        link_name, _ = split_link(key)
        link_count = max([len(row[link_name]) for row in entities])
        if link_count > 1:
            link_titles = (
                get_link_name(split_link_alias(key)[0], column)
                for column in xrange(link_count)
            )
            titles.extend(link_titles)
        else:
            titles.append(key)

    return titles


def is_link_field(field):
    """Return boolean whether field should be considered a link."""
    return '.' in field


def split_link(link):
    """Return (link_name, link_alias) given link name."""
    return link.split('.', 1)


def split_link_alias(link_alias):
    """Return (alias_root, link_id) from link alias."""
    alias_root, alias_id = link_alias.split('#', 1)
    return alias_root, int(alias_id)


def is_link_plural(key):
    """Return true if link represents a to_many relationship."""
    if not is_link_field(key):
        log.warning('is_link_singular() called on non-link: %s', key)

    return '#' in key


def get_tsv_dict_plural_links(entity, link_titles):
    """Return plural link portion of tsv dict for single entity."""
    tsv_dict = {}
    plural_links = [
        link for link in link_titles
        if is_link_plural(link)
    ]

    for key in plural_links:
        tsv_dict[key] = ''
        link_name, link_alias = split_link(key)
        alias_root, link_id = split_link_alias(link_alias)

        if link_name not in entity:
            continue

        link = entity[link_name]
        if len(link) >= link_id and alias_root in link[link_id-1]:
            val = link[int(link_id)-1][alias_root]
            tsv_dict[key] = list_to_comma_string(val)

    return tsv_dict


def get_tsv_dict_singular_links(entity, link_titles):
    """Return singular link portion of tsv dict for single entity."""
    tsv_dict = {}
    singular_links = [
        link for link in link_titles
        if not is_link_plural(link)
    ]

    for key in singular_links:
        link_name, link_alias = split_link(key)
        links = entity[link_name]

        if len(links) > 1:
            log.warning('called on plural links: %s', links)

        if links:
            tsv_dict[key] = list_to_comma_string(links[0][link_alias])
        else:
            tsv_dict[key] = ''

    return tsv_dict


def get_node_link_json(node, props):
    """Return the fields in the node json from links"""

    link_props = get_link_props(props)
    entity, links = {}, {}

    for link in link_props:
        edge_name, alias = split_link(link)
        alias_root = alias.split('#', 1)[0]

        if edge_name in links:
            links[edge_name].append(alias_root)
        else:
            links[edge_name] = [alias_root]

    for edge_name, aliases in links.iteritems():
        edges = getattr(node, edge_name, [])
        edge_aliases = [
            {
                alias: (edge.node_id if alias == 'id' else edge[alias])
                for alias in aliases
            } for edge in edges
        ]
        entity[edge_name] = edge_aliases

    return entity


def get_node_non_link_json(node, props):
    """Return the fields in the node json that are not links"""
    non_link_props = get_non_link_props(props)
    entity = {}

    for key in non_link_props:
        if key == 'type':
            entity[key] = node.label
        elif key == 'id':
            entity[key] = node.node_id
        else:
            entity[key] = node[key]

    return entity


def list_to_comma_string(val):
    """
    Handle array fields by converting them to a comma-separated string.

    Example:
        ['1','2','3'] -> 1,2,3
    """

    if val is None:
        return ''

    if isinstance(val, list):
        val = ','.join(val)
    return val


def get_tsv_dict_non_links(entity, non_link_titles):
    """Return non-link portion of tsv dict for single entity."""
    return {
        key: list_to_comma_string(entity[key])
        for key in non_link_titles
    }


def get_tabular_entity(entity, link_titles, non_link_titles):
    """Return tsv_dict for a single entity"""

    tsv_dict = {}
    tsv_dict.update(get_tsv_dict_plural_links(entity, link_titles))
    tsv_dict.update(get_tsv_dict_singular_links(entity, link_titles))
    tsv_dict.update(get_tsv_dict_non_links(entity, non_link_titles))

    return tsv_dict


def get_tsv_dicts(entities, titles):
    """Return a generator of tsv_dicts given iterable :param:`entities`."""
    link_titles = get_link_props(titles)
    non_link_titles = get_non_link_props(titles)
    for entity in entities:
        yield get_tabular_entity(entity, link_titles, non_link_titles)


def entity_to_template_str(label, file_format, **kwargs):
    """Return template based on file_format for a given node label."""
    template = entity_to_template(label, file_format=file_format)
    if file_format == 'json':
        return json_dumps_formatted(template)
    elif file_format in DELIMITERS:
        output = StringIO.StringIO()
        writer = csv.writer(output, delimiter=DELIMITERS[file_format])
        writer.writerow(template)
        writer.writerow(tsv_example_row(label, template))
        return output.getvalue()
    else:
        raise UnsupportedError(file_format)


def tsv_example_row(label, title):
    """Return row with entity type column populated"""

    return [label] + [""] * (len(title) - 1)


def json_dumps_formatted(data):
    """Return json string with standard format"""
    return json.dumps(
        data, indent=2,
        separators=(', ', ': '), ensure_ascii=False
        ).encode('utf-8')


def get_json_template(entity_types):
    """Return json template for entity types"""

    return json_dumps_formatted([
        entity_to_template(entity_type, file_format='json')
        for entity_type in entity_types
    ])


def get_delimited_template(entity_types, file_format, filename=TEMPLATE_NAME):
    """Return :param:`file_format` (TSV or CSV) template for entity types."""
    tar_obj = StringIO.StringIO()
    tar = tarfile.open(filename, mode='w|gz', fileobj=tar_obj)

    for entity_type in entity_types:
        content = entity_to_template_str(entity_type, file_format=file_format)
        partname = '{}.{}'.format(entity_type, file_format)
        tarinfo = tarfile.TarInfo(name=partname)
        tarinfo.size = len(content)
        tar.addfile(tarinfo, StringIO.StringIO(content))

    tar.close()
    return tar_obj.getvalue()


def get_all_template(file_format, categories=None, exclude=None, **kwargs):
    """
    Return template in format `file_format` for given categories.

    ..note: kwargs absorbs `project`, `program` intended for future use
    """
    categories = categories.split(',') if categories else []
    exclude = exclude.split(',') if exclude else []
    entity_types = [
        entity_type
        for entity_type, schema in dictionary.schema.iteritems()
        if 'project_id' in schema.get('properties', {})
        and (not categories or schema['category'] in categories)
        and (not exclude or entity_type not in exclude)
    ]
    if file_format == 'json':
        return get_json_template(entity_types)
    else:
        return get_delimited_template(entity_types, file_format)


def _get_links_json(link, exclude_id):
    """Return parsed link template from link schema in json form."""
    target_schema = dictionary.schema[link['target_type']]
    link_template = dict({
        k: None for subkeys in target_schema.get('uniqueKeys', [])
        for k in subkeys
    })
    if 'project_id' in link_template:
        del link_template['project_id']
    if exclude_id:
        del link_template['id']
    return link_template


def _get_links_delimited(link, exclude_id):
    """Return parsed link template from link schema in delimited form."""
    link_template = []
    target_schema = dictionary.schema[link['target_type']]
    # add a #1 to the link to indicate it's a many relationship
    to_many = link['multiplicity'] in ['many_to_many', 'one_to_many']
    postfix = "#1" if to_many else ""

    # default key for link is the GDC ID
    if not exclude_id:
        link_template.append('id'+postfix)

    unique_keys = [
        key for key in target_schema['uniqueKeys']
        if key != ['id']
    ]

    for unique_key in unique_keys:
        keys = copy.copy(unique_key)
        if 'project_id' in keys:
            keys.remove('project_id')
        link_template += [prop + postfix for prop in keys]

        # right now we only have one alias for each entity,
        # so we pick the first one for now
        break

    return link_template


def _get_links(file_format, schema, exclude_id):
    """Parse links from schema

    we don't have project specific schema now
    so right now this uses top level schema

    """

    links = dict()

    subgroups = [link for link in schema if 'subgroup' in link]
    non_subgroups = [link for link in schema if 'name' in link]

    for link in non_subgroups:
        if file_format == 'json':
            links[link['name']] = _get_links_json(link, exclude_id)
        else:
            links[link['name']] = _get_links_delimited(link, exclude_id)

    for subgroup in subgroups:
        links.update(_get_links(file_format, subgroup['subgroup'], exclude_id))

    return links


def is_property_hidden(key, schema, exclude_id):
    """Boolean whether key should be hidden"""

    is_system_prop = (
        key in schema['systemProperties'] and
        key not in ['id', 'project_id'])
        # TODO Make this a configurable blacklist

    if is_system_prop:
        return True

    elif exclude_id and key == 'id':
        return True

    return False


def entity_to_template(label, exclude_id=True, file_format='tsv', **kwargs):
    """Return template dict for given label."""
    if label not in dictionary.schema:
        raise NotFoundError(
            'Entity type {} is not in dictionary'.format(label)
        )
    if file_format not in SUPPORTED_FORMATS:
        raise UnsupportedError(file_format)
    schema = dictionary.schema[label]
    links = _get_links(file_format, schema['links'], exclude_id)
    if file_format == 'json':
        return entity_to_template_json(links, schema, exclude_id)
    else:
        return entity_to_template_delimited(links, schema, exclude_id)


def entity_to_template_json(links, schema, exclude_id):
    keys = {}
    properties = {
        key
        for key in schema['properties']
        if not is_property_hidden(key, schema, exclude_id)
    }
    for key in properties:
        if 'required' in schema and key in schema['required']:
            marked_key = '*' + key
        else:
            marked_key = key
        if key in links:
            keys[marked_key] = links[key]
        elif key == 'type':
            keys[marked_key] = schema['id']
        else:
            keys[marked_key] = None
    # users need to submit the 'urls' field, but 'urls' is not in the schema
    # (since it is a property of records in indexd, and is not in the dict).
    # So we are adding it to the templates here.
    if schema['category'] == 'data_file':
        keys['*urls'] = None
    return keys


def entity_to_template_delimited(links, schema, exclude_id):
    """Return ordered header for delimited export."""
    ordered = ['type', 'id']
    ordered_unique_keys = {
        key
        for unique_keys in schema.get('uniqueKeys', [])
        for key in unique_keys if key not in ordered
    }

    ordered += sorted(ordered_unique_keys)
    remaining_keys = set(schema['properties']) - set(ordered)

    unordered_links = set()
    unordered_required = set()
    unordered_optional = set()

    for key in remaining_keys:
        if key in links:
            unordered_links.add(key)
        elif key in schema.get('required', []):
            unordered_required.add(key)
        else:
            unordered_optional.add(key)

    ordered.extend(sorted(unordered_links))
    ordered.extend(sorted(unordered_required))
    ordered.extend(sorted(unordered_optional))

    # TODO FIXME ordered is not ordered at this point.
    # just the concatenation of 4 ordered lists
    keys = []
    visible_keys = [
        key
        for key in ordered
        if not is_property_hidden(key, schema, exclude_id)
    ]
    for key in visible_keys:
        if 'required' in schema and key in schema['required']:
            marked_key = '*' + key
        else:
            marked_key = key
        if key in links:
            for prop in links[key]:
                keys.append(marked_key + '.' + prop)
        else:
            keys.append(marked_key)
    # users need to submit the 'urls' field, but 'urls' is not in the schema
    # (since it is a property of records in indexd, and is not in the dict).
    # So we are adding it to the templates here.
    if schema['category'] == 'data_file':
        keys.append('*urls')

    return keys


class ExportFile(object):
    """
    Export entities to tsv or csv or json.
    An ExportFile should be intantiated and then call get_response to pass the
    generator to flask response.
    """

    def __init__(
            self, ids=None, with_children=False, file_format='tsv',
            category=None, program=None, project=None, **kwargs):

        self.file_format = file_format
        self.program = program
        self.project = project

        if not self.program:
            raise InternalError("Unknown program")

        if not self.project:
            raise InternalError("Unknown project")

        self.project_id = '{}-{}'.format(self.program, self.project)
        # The result contains a dictionary which  stores json or string buffer
        # for each type of entity.
        self.result = defaultdict(list)
        self.templates = dict()
        self.category = category
        self.get_nodes(ids, with_children)
        self._buffer = StringIO.StringIO()

    def write(self, data):
        """Write data do internal buffer."""
        self._buffer.write(data)

    def tell(self):  # pylint: disable=R0201
        """Stub to be file-like."""
        return 0

    def seek(self, offset, whence=None):  # pylint: disable=R0201,W0613
        """Stub to be file-like."""
        return 0

    def getvalue(self):
        """Return buffer contents."""
        return self._buffer.getvalue()

    def reset(self):
        """Clear buffer."""
        self._buffer.close()
        self._buffer = StringIO.StringIO()

    def get_nodes(self, ids, with_children):
        """Look up nodes and set self.result"""
        ids = parse_ids(ids)
        with flask.current_app.db.session_scope():
            self.nodes = (
                flask.current_app.db.nodes().ids(ids)
                .props(project_id=self.project_id)
                .all()
            )
            found_ids = {node.node_id for node in self.nodes}
            if not found_ids:
                raise NotFoundError("Unable to find {}".format(', '.join(ids)))
            missing_ids = set(ids) - found_ids
            if missing_ids:
                log.warn("Unable to find: %s", ', '.join(missing_ids))
            if with_children:
                parents = copy.copy(self.nodes)
                for node in parents:
                    self.get_entity_tree(node, self.nodes)

            self.get_dictionary()

    def get_entity_tree(self, node, visited):
        """
        Accumulate child nodes in :param:`visited`.

        Walk down spanning tree of graph, traversing to edges_in and filtering
        by self.category.
        """
        for edge in node.edges_in:
            if edge.src.props.get('project_id') != node.project_id:
                log.warn(
                    "skip edge %s for %s that's not in project %s", str(edge),
                    str(node), str(node.project_id)
                )
                continue
            if edge.src in visited:
                continue
            should_add = (
                not self.category
                or self.category == edge.src._dictionary['category']
            )
            if should_add:
                visited.append(edge.src)
            self.get_entity_tree(edge.src, visited)

    @property
    def is_json(self):
        """Return bool whether format is json."""
        return self.file_format == 'json'

    @property
    def is_delimited(self):
        """Return bool whether format is delimited."""
        return self.file_format in DELIMITERS

    @property
    def is_singular(self):
        """Return bool if there is a single result."""
        return len(self.result) == 1

    @property
    def filename(self):
        """Return a filename string based on format and number of results."""
        if not self.result:
            raise InternalError(
                "Unable to determine file name with no results"
            )

        if self.is_delimited and self.is_singular:
            return '{}.{}'.format(self.result.keys()[0], self.file_format)
        elif self.is_delimited:
            return 'gdc_export_{}.tar.gz'.format(self._get_sha())
        elif self.is_json and self.is_singular:
            return '{}.json'.format(self.result.keys()[0])
        elif self.is_json:
            return 'gdc_export_{}.json'.format(self._get_sha())
        else:
            raise UserError('Format {} not supported'.format(self.file_format))

    def _get_sha(self):
        """Return a unique hash for this export."""
        sha = hashlib.sha1(str(time.time()))
        for node in self.nodes:
            sha.update(node.node_id)
        return sha.hexdigest()

    def get_tabular(self):
        """
        Set the state of self.result[label] to filelike object for label in
        self.result json.
        """
        delimiter = DELIMITERS[self.file_format]
        json_output, self.result = self.result, {}

        def encode(string):
            """Encode keys or values for writing a tsv dict (see below)."""
            if isinstance(string, unicode):
                return string.encode('utf-8')
            else:
                return string

        for label, entities in json_output.iteritems():
            template = self.templates[label]
            titles = []
            titles.extend(get_link_titles(entities, template))
            titles.extend(get_non_link_props(template))
            buff = StringIO.StringIO()
            writer = csv.DictWriter(buff, titles, delimiter=delimiter)
            self.result[label] = buff
            writer.writeheader()

            for tsv_dict in get_tsv_dicts(entities, titles):
                writer.writerow(
                    {encode(k): encode(v) for k, v in tsv_dict.iteritems()}
                )

    def get_delimited_response(self):
        """Yield delimited string per result."""
        self.get_tabular()
        if self.is_singular:
            yield self.result.values()[0].getvalue()
        else:
            tar = tarfile.open(self.filename, mode='w|gz', fileobj=self)
            for label, entities in self.result.iteritems():
                partname = '{}.{}'.format(label, self.file_format)
                info = tarfile.TarInfo(name=partname)
                content = entities.getvalue()
                info.size = len(content)
                tar.addfile(info, StringIO.StringIO(content))
                yield self.getvalue()
                self.reset()
            tar.close()
            yield self.getvalue()

    def get_json_response(self):
        """Yield single json string."""
        # Throw away the keys because re-upload is not expecting them.
        yield json_dumps_formatted(
            [r for v in self.result.values() for r in v]
        )

    def get_response(self):
        """Return response based on format and number of results."""
        if self.is_delimited:
            return self.get_delimited_response()
        elif self.is_json:
            return self.get_json_response()
        else:
            raise UnsupportedError(self.file_format)

    def get_node_dictionary(self, node):
        """Return the json doc for a single node."""
        entity = {'id': node.node_id}
        props = self.templates.get(node.label)
        if not props:
            props = entity_to_template(
                node.label,
                program=self.program,
                project=self.project,
                exclude_id=False,
            )
            self.templates[node.label] = props
        # 'urls' is part of the templates but not part of the dicts
        # and not exported, so we remove it here
        if '*urls' in props:
            props.remove('*urls')
        stripped_props = []
        for prop in props:
            if prop[0] == '*':
                stripped_props.append(prop[1:])
            else:
                stripped_props.append(prop)
        entity.update(get_node_link_json(node, stripped_props))
        entity.update(get_node_non_link_json(node, stripped_props))
        return entity

    def get_dictionary(self):
        """Return export as a dictionary."""
        for node in self.nodes:
            node_json = self.get_node_dictionary(node)
            self.result[node.label].append(node_json)
        return self.result


def validate_export_node(node_label):
    """
    Raise a ``UserError`` if there is any reason that nodes with the type
    specified by ``node_label`` should not be exported. This m

    Args:
        node_label (str): string of the node type

    Return:
        None

    Raises:
        UserError: if the node cannot be exported
    """
    if node_label not in dictionary.schema:
        raise UserError(
            'dictionary does not have node with type {}'.format(node_label)
        )
    category = get_node_category(node_label)
    if category in UNSUPPORTED_EXPORT_NODE_CATEGORIES:
        raise UserError('cannot export node with category `internal`')


def export_all(node_label, project_id, file_format, db, **kwargs):
    """
    Export all nodes of type with name ``node_label`` to a TSV file and yield
    rows of the resulting TSV.

    Args:
        node_label (str): type of nodes to look up, for example ``'case'``
        project_id (str): project to look under
        file_format (str): json or tsv
        db (psqlgraph.PsqlGraphDriver): database driver to use for queries

    Return:
        Generator[str]: generator of rows of the TSV file

    Example:
        Example of streaming a TSV in a Flask response using this function:

        .. code-block:: python

            return flask.Response(export_all(
                'case', 'acct-test', flask.current_app.db
            ))
    """
    # Examples in coments throughout function will start from ``'case'`` as an
    # example ``node_label`` (so ``gdcdatamodel.models.Case`` is the example
    # class).

    with db.session_scope() as session:

        cls = psqlgraph.Node.get_subclass(node_label)

        # Get the template for this node, which is basically the column
        # headers in the resulting TSV.
        unstripped_template = entity_to_template(
            node_label, exclude_id=False, file_format='tsv'
        )
        # Strip asterisks.
        template = []
        for prop in unstripped_template:
            if prop[0] == '*':
                template.append(prop[1:])
            else:
                template.append(prop)
        # Get the titles so the linked fields are at the end (to match the
        # structure of the query we will run later).
        titles_non_linked = []
        titles_linked = []
        for title in template:
            if is_link_field(title):
                titles_linked.append(title)
            else:
                titles_non_linked.append(title)
        titles = titles_non_linked + titles_linked
        # Example ``titles``:
        #
        #     [
        #         'type', 'id', 'submitter_id', 'disease_type', 'primary_site',
        #         'experiments.id', 'experiments.submitter_id'
        #     ]

        def format_prop(prop):
            """
            Map over ``titles`` to get properties usable for looking up from
            node instances.

            Change properties to have 'node_id' instead of 'id', and 'label'
            instead of 'type', so that the props can be looked up as dictionary
            entries:

            .. code-block:: python

                node[prop]

            For properties which link to other nodes, convert the link to a
            tuple such that the linked property can be looked up using the
            elements of the tuple as indexes. For example, for a ``Case`` node
            with a link to `experiments.id`:

            .. code-block:: python

                # prop == ('experiments', 0, 'node_id')
                node[prop[0]][prop[1]][prop[2]]
            """
            if prop == 'id':
                return 'node_id'
            elif prop == 'type':
                return 'label'
            elif is_link_field(prop):
                link_name, link_alias = split_link(prop)
                if is_link_plural(prop):
                    alias_root, _ = split_link_alias(link_alias)
                    return (link_name, format_prop(alias_root))
                else:
                    return (link_name, format_prop(link_alias))
            return prop

        # ``props`` is just a list of strings of the properties of the node
        # class that should go in the result.
        props = []
        # ``linked_props`` is a list of attributes belonging to linked classes
        # (for example, ``Experiment.node_id``).
        linked_props = []
        # Example ``cls._pg_links`` for reference:
        #
        #     Case._pg_links == {
        #         'experiments': {
        #             'dst_type': gdcdatamodel.models.Experiment,
        #             'edge_out': '_CaseMemberOfExperiment_out',
        #         }
        #     }
        #
        # This is used to look up the classes for the linked nodes.
        # Now, fill out the properties lists from the titles.
        for prop in map(format_prop, titles):
            if type(prop) is tuple:
                link_name, link_prop = prop
                link_cls = cls._pg_links[link_name]['dst_type']
                linked_props.append(getattr(link_cls, link_prop))
            else:
                props.append(prop)

        # Build up the query. The query will contain, firstly, the node class,
        # and secondly, all the relevant properties in linked nodes.
        query_args = [cls] + linked_props
        query = session.query(*query_args).prop('project_id', project_id)
        # Join the related node tables using the links.
        for link in cls._pg_links.values():
            query = (
                query
                .outerjoin(link['edge_out'])
                .outerjoin(link['dst_type'])
            )
        # The result from the query should look like this (header just for
        # example):
        #
        # Case instance          experiments.id   experiments.submitter_id
        # (<Case(...[uuid]...)>, u'...[uuid]...', u'exp-01')

        if file_format == "json":
            yield '{ "data": ['
        else:  # json
            # Yield the lines of the file.
            yield '{}\n'.format('\t'.join(titles))

        js_list_separator = ''
        for result in query.yield_per(1000):
            row = []
            json_obj = {"link_fields": {}}
            # Write in the properties from just the node.
            node = result[0]
            for prop in props:
                val = list_to_comma_string(node[prop])
                row.append(val)
                json_obj[prop] = val
            # Tack on the linked properties.
            row.extend(map(lambda col: list_to_comma_string(col), result[1:]))
            for idx, title in enumerate(titles_linked):
                json_obj["link_fields"][title] = result[idx + 1] or ''
            # Convert row elements to string if they are not
            for idx, val in enumerate(row):
                if not isinstance(val, str):
                    row[idx] = str(row[idx])
            if file_format == "json":
                yield js_list_separator + json.dumps(json_obj)
            else:
                yield '{}\n'.format('\t'.join(row))
            js_list_separator = ','

        if file_format == "json":
            yield ']}'
