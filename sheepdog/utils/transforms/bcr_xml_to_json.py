# pylint: disable=no-member
# pylint: disable=unsubscriptable-object
"""
Defines :class:`.BcrClinicalXmlToJsonParser`, a class (that is instantiated
with a given project_code) which consumes BCR Clinical XML and produces JSON.

Pylint ``no-member`` error is disabled because for some reason there are a lot
of false positives with ``lxml.etree``.
"""

import datetime
import json
import math
import pkg_resources
from uuid import uuid5, UUID

from cdislogging import get_logger
import flask

# TODO: consider switching lxml to https://pypi.org/project/defusedxml/#defusedxml-sax
from lxml import etree  # nosec
import requests
import yaml

from sheepdog import dictionary
from sheepdog.errors import ParsingError, SchemaError
from sheepdog.globals import BCR_MAPPING


log = get_logger(__name__)
SCHEMA_LOCATION_WHITELIST = [
    "https://github.com/nchbcr/xsd",
    "http://tcga-data.nci.nih.gov",
]


def _parse_schema_location(root):
    """Get all schema locations from xml."""
    try:
        namespace = root.nsmap["xsi"]
    except Exception as e:
        raise SchemaError("Can't get schema location namespace", e)
    try:
        schema_location = root.attrib["{%s}schemaLocation" % namespace]
    except Exception as e:
        raise SchemaError("Missing xsi:schemaLocation", e)
    # schemaLocation is a space delimited list of namespace and location pairs
    # return odd elements
    locations = schema_location.split(" ")
    if len(locations) >= 2 and len(locations) % 2 == 0:
        return locations[1::2]
    else:
        raise SchemaError("schemaLocation has to be a list of namespace and url pairs")


def _fetch_schema(schema_url):
    """Fetch schema using the url from schemaLocation."""
    if not any(map(schema_url.startswith, SCHEMA_LOCATION_WHITELIST)):
        raise SchemaError("schema location: {} is not allowed".format(schema_url))
    try:
        r = requests.get(
            schema_url, proxies=flask.current_app.config.get("EXTERNAL_PROXIES")
        )
    except Exception as e:
        raise SchemaError("Can't get xml XSD at {}".format(schema_url), e)
    if r.status_code == 200:
        try:
            return etree.XMLSchema(etree.XML(r.text.encode("utf-8")))
        except Exception as e:
            raise SchemaError("Invalid XML XSD at {}".format(schema_url), e)
    else:
        raise SchemaError("Can't get XML XSD at {}: {}".format(schema_url, r.text))


def validated_parse(xml):
    """
    Parse an XML document or fragment from a string and return the root node.
    """
    try:
        # TODO: consider switching lxml to https://pypi.org/project/defusedxml/#defusedxml-sax
        root = etree.fromstring(xml)  # nosec
        # note(pyt): return the document without doing schema validation
        # until we are clear about how to handle the xsd
        return root
    except etree.XMLSyntaxError as msg:
        log.error("User submitted invalid xml: {}".format(msg))
        raise
    schemas = map(_fetch_schema, _parse_schema_location(root))
    try:
        for schema in schemas:
            schema.assertValid(root)
        return root
    except (etree.XMLSchemaError, etree.DocumentInvalid) as msg:
        log.error("User submitted invalid xml: {}".format(msg))
        # note(jsm): Here we re-raise. This exception should be
        # caught by the caller, at the time this comment was
        # written, it will be caught in
        # ``..transaction.handle_xml_transaction``
        raise


def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return int(delta.total_seconds())


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def to_bool(val):
    possible_true_values = ["true", "yes"]
    possible_false_values = ["false", "no"]

    if val is None:
        return None
    if val.lower() in possible_true_values:
        return True
    elif val.lower() in possible_false_values:
        return False
    else:
        raise ValueError("Cannot convert {} to boolean".format(val))


class BcrXmlToJsonParser(object):
    def __init__(self, project):
        """
        Create a parser to convert XML to GDC JSON.

        Args:
            project (str): the id of the project node to link cases to
        """
        self.project = project
        self.namespaces = None
        self.exported_entitys = 0
        self.export_count = 0
        self.ignore_missing_properties = True
        self.xml_mapping = json.loads(
            json.dumps(yaml.safe_load(BCR_MAPPING)), object_hook=AttrDict
        )
        self.entities = {}

    def xpath(
        self,
        path,
        root=None,
        single=False,
        nullable=True,
        expected=True,
        text=True,
        label="",
    ):
        """
        Wrapper to perform the xpath queries on the xml

        Args:
            path (str): The xpath location path
            root: the lxml element to perform query on
            single (bool): raise ParsingError if the result is not singular
            nullable (bool): raise ParsingError if the result is null
            expected (bool): raise ParsingError if the result does not exist
            text (bool): whether the return value is the .text str value
            label (str): label for logging

        Return:

        Raises:
            ParsingError:
                if ``single``, ``nullable``, or ``expected`` are True and their
                respective conditions are violated (see above)
        """
        if root is None:
            root = self.xml_root
        try:
            result = root.xpath(path, namespaces=self.namespaces)
        except etree.XPathEvalError:
            result = []
        except:
            raise
        rlen = len(result)
        if rlen < 1 and expected:
            raise ParsingError("{}: Unable to find xpath {}".format(label, path))

        if rlen < 1 and not expected and single:
            return None

        if rlen < 1 and not expected and not single:
            return []

        elif rlen > 1 and single:
            log.error(result)
            msg = "{}: Expected 1 result for xpath {}, found {}"
            raise ParsingError(msg.format(label, path, result))

        if text:
            result = [r.text for r in result]
            if not nullable and None in result:
                raise ParsingError("{}: Null result for {}".format(label, result))

        if single:
            result = result[0]

        return result

    def loads(self, xml):
        """
        Take xml string and convert it to a graph to insert into psqlgraph.

        Args:
            xml (str): xml string to convert and insert

        Return:
            self
        """
        if not xml:
            return None

        self.xml_root = validated_parse(str(xml)).getroottree()
        self.namespaces = self.xml_root.getroot().nsmap
        for entity_type, param_list in self.xml_mapping.items():
            for params in param_list:
                self.parse_entity(entity_type, params)

        return self

    def dumps(self, indent=2):
        return json.dumps(self.json, indent=indent)

    @property
    def json(self):
        """Return list of entities values."""
        return list(self.entities.values())

    def parse_entity(self, entity_type, params):
        """
        Convert a subsection of the xml that will be treated as an entity.

        Args:
            entity_type (str): the type of entity to be used as a label
            params (dict):
                the parameters that govern xpath queries and translation from
                the translation yaml file

        Return:
            None
        """
        roots = self.get_entity_roots(entity_type, params)
        for root in roots:
            # Get entity and entity properties
            entity_id = self.get_entity_id(root, entity_type, params)
            args = (root, entity_type, params, entity_id)
            props = self.get_entity_properties(*args)
            props.update(self.get_entity_datetime_properties(*args))
            props.update(self.get_entity_const_properties(*args))

            # Get edges to and from this entity
            edges = self.get_entity_edges(root, entity_type, params, entity_id)
            props.update(edges)

            # If the entity is a case, supliement the edges with an edge
            # to the project
            if entity_type == "case":
                props["projects"] = [{"id": self.project}]

            self.save_entity(entity_id, entity_type, props)

    def save_entity(self, entity_id, label, properties):
        """Adds a entity to the graph"""

        if label == "file":
            raise ParsingError("This endpoint is not built to handle file entities")

        if entity_id in self.entities:
            self.entities[entity_id].update(properties)
        else:
            self.entities[entity_id] = dict(id=entity_id, type=label, **properties)

    def get_entity_roots(self, entity_type, params, root=None):
        """
        Return a list of xml entity root elements for a given entity_type.

        Args:
            entity_type (str): entity type to be used as a label in psqlgraph
            params (dict):
                parameters that govern xpath queries and translation from the
                translation yaml file
        """
        if not params.root:
            log.warn("No root xpath for {}".format(entity_type))
            return
        xml_entities = self.xpath(
            params.root, root=root, expected=False, text=False, label="get_entity_roots"
        )
        return xml_entities

    def get_entity_id(self, root, entity_type, params):
        """
        Look up the id for the entity.

        Args:
            root: the lxml root element to treat as a entity
            entity_type (str): entity type to be used as a label in psqlgraph
            params (dict):
                the parameters that govern xpath queries and translation from
                the translation yaml file

        Return:
            str: the entity id
        """
        assert not (
            "id" in params and "generated_id" in params
        ), "Specification of an id xpath and parameters for generating an id"
        # Lookup ID
        if "id" in params:
            entity_id = self.xpath(
                params.id, root, single=True, label=entity_type
            ).lower()
        else:
            entity_id = None
        return entity_id

    def get_entity_properties(self, root, entity_type, params, entity_id=""):
        """
        For each parameter in the setting file, try to look it up, and add
        it to the entity properties.

        Args:
            root: the lxml root element to treat as a entity
            entity_type (str):
                the entity type to be used as a label in psqlgraph
            params (dict):
                the parameters that govern xpath queries and translation
                from the translation yaml file
            entity_id (str): used for logging

        Return:
            dict: the entity properties
        """
        if "properties" not in params or not params.properties:
            return {}

        props = {}
        schema = dictionary.schema[entity_type]
        for prop, args in params.properties.items():
            if args is None:
                if "null" in schema["properties"][prop].get("type", []):
                    props[prop] = None
                continue
            path, _type = args["path"], args["type"]
            if not path:
                if "null" in schema["properties"][prop].get("type", []):
                    props[prop] = None
                continue
            result = self.xpath(
                path,
                root,
                single=True,
                text=True,
                expected=(not self.ignore_missing_properties),
                label="{}: {}".format(entity_type, entity_id),
            )
            # optional null fields are removed
            if result is None and prop not in dictionary.schema[entity_type].get(
                "required", []
            ):
                continue
            props[prop] = munge_property(result, _type)
        return props

    def get_entity_const_properties(
        self, root, entity_type, params, entity_id=""
    ):  # pylint: disable=R0201
        """
        For each parameter in the setting file that is a constant value, add it
        to the properties dict.

        Args:
            root: the lxml root element to treat as a entity
            entity_type (str):
                the entity type to be used as a label in psqlgraph
            params (dict):
                the parameters that govern xpath queries and translation
                from the translation yaml file
            entity_id (str): used for logging

        Return:
            dict: dictionary of properties
        """
        if "const_properties" not in params or not params.const_properties:
            return {}
        props = {}
        for prop, args in params.const_properties.items():
            props[prop] = munge_property(args["value"], args["type"])
        return props

    def get_entity_datetime_properties(self, root, entity_type, params, entity_id=""):
        """
        For datetime each parameter in the setting file, try and look it up,
        and add it to the entity properties.

        Args:
            root: the lxml root element to treat as a entity
            entity_type (str): entity type to be used as a label in psqlgraph
            params (dict):
                the parameters that govern xpath queries and translation from
                the translation yaml file
            entity_id (str): used for logging

        Return:
            dict: the properties dictionary
        """
        if "datetime_properties" not in params or not params.datetime_properties:
            return {}

        props = {}
        # Loop over all given datetime properties
        for name, timespans in params.datetime_properties.items():
            times = {"year": 0, "month": 0, "day": 0}
            # Parse the year, month, day
            for span in times:
                if span in timespans:
                    temp = self.xpath(
                        timespans[span],
                        root,
                        single=True,
                        text=True,
                        label="{}: {}".format(entity_type, entity_id),
                    )
                    times[span] = 0 if temp is None else int(temp)

            if not times["year"]:
                props[name] = 0
            else:
                props[name] = unix_time(
                    datetime.datetime(times["year"], times["month"], times["day"])
                )

        return props

    def get_entity_edges(self, *args, **kwargs):
        """
        For each edge type in the settings file, lookup the possible edges.

        Args:
            root: the lxml root element to treat as a entity
            entity_type (str): entity type to be used as a label in psqlgraph
            params (dict):
                the parameters that govern xpath queries and translation from
                the translation yaml file
            entity_id (str): used for logging

        Return:
            dict: a dictionary of edges
        """
        edges = {}
        edges.update(self.get_entity_edges_by_properties(*args, **kwargs))
        edges.update(self.get_entity_edges_by_id(*args, **kwargs))
        return edges

    def get_entity_edges_by_id(self, root, entity_type, params, entity_id=""):
        """
        For each edge type in the settings file, lookup the possible edges
        using the entity id.

        Args:
            root: the lxml root element to treat as a entity
            entity_type (str): entity type to be used as a label in psqlgraph
            params (dict):
                the parameters that govern xpath queries and translation from
                the translation yaml file
            entity_id (str): used for logging

        Return:
            dict: dictionary mapping edge types to lists of entity ids
        """
        edges = {}
        if "edges" not in params or not params.edges:
            return edges
        for edge_type, path in params.edges.items():
            results = self.xpath(
                path,
                root,
                expected=False,
                text=True,
                label="{}: {}".format(entity_type, entity_id),
            )
            if results:
                edges[edge_type] = [{"id": r.lower()} for r in results]
        return edges

    def get_entity_edges_by_properties(
        self, root, entity_type, params, entity_id=""
    ):  # pylint: disable=R0201
        """
        For each edge type in the settings file, lookup the possible edges

        Args:
            root: the lxml root element to treat as a entity
            entity_type (str): entity type to be used as a label in psqlgraph
            params (dict):
                the parameters that govern xpath queries and translation from
                the translation yaml file
            entity_id (str): used for logging

        Return:
            dict: dictionary mapping entity ids to (label, edge_type) pairs
        """
        edges = {}
        if "edges_by_property" not in params or not params.edges_by_property:
            return edges
        return edges

    def get_entity_edge_properties(self, root, edge_type, params, entity_id=""):
        if (
            "edge_properties" not in params
            or not params.edge_properties
            or edge_type not in params.edge_properties
        ):
            return {}

        props = {}
        for prop, args in params.edge_properties[edge_type].items():
            path, _type = args["path"], args["type"]
            if not path:
                continue
            result = self.xpath(
                path,
                root,
                single=True,
                text=True,
                expected=(not self.ignore_missing_properties),
                label="{}: {}".format(edge_type, entity_id),
            )
            props[prop] = munge_property(result, _type)
        return props

    def get_entity_edge_datetime_properties(
        self, root, edge_type, params, entity_id=""
    ):

        if (
            "edge_datetime_properties" not in params
            or not params.edge_datetime_properties
            or edge_type not in params.edge_datetime_properties
        ):
            return {}

        props = {}
        # Loop over all given datetime properties
        for name, timespans in params.edge_datetime_properties[edge_type].items():
            times = {"year": 0, "month": 0, "day": 0}

            # Parse the year, month, day
            for span in times:
                if span in timespans:
                    temp = self.xpath(
                        timespans[span],
                        root,
                        single=True,
                        text=True,
                        expected=True,
                        label="{}: {}".format(edge_type, entity_id),
                    )
                    times[span] = 0 if temp is None else int(temp)

            if not times["year"]:
                props[name] = 0
            else:
                props[name] = unix_time(
                    datetime.datetime(times["year"], times["month"], times["day"])
                )
        return props


class BcrClinicalXmlToJsonParser(object):
    def __init__(self, project_code, mapping=None):
        if mapping is None:
            mapping = pkg_resources.resource_string(
                "gdcdatamodel", "xml_mappings/tcga_clinical.yaml"
            )
        self.xpath_ref = yaml.safe_load(mapping)
        self.docs = []

    @property
    def json(self):
        """Return list of docs."""
        return self.docs

    def get_xml_roots(
        self, root, path, namespaces, nullable=False
    ):  # pylint: disable=R0201
        roots = root.xpath(path, namespaces=namespaces)
        if not roots and not nullable:
            raise Exception("Can't find xml root {}".format(path))
        return roots

    def xpath(
        self, root, path, namespaces, nullable=True, suffix=""
    ):  # pylint: disable=R0201
        result = root.xpath(path, namespaces=namespaces)

        if hasattr(result, "__iter__"):
            rlen = len(result)
            if rlen == 0:
                if nullable:
                    return None
                else:
                    raise Exception("Can't fine element {}".format(path))
            elif rlen > 1:
                raise Exception("More than one {} is found".format(path))
            else:
                result = result[0].text

        if result is None and not nullable:
            raise Exception("Can't find element {}".format(path))

        if suffix:
            result = str(result) + suffix
        return result

    def loads(self, doc):
        doc_root = validated_parse(str(doc))
        namespaces = doc_root.nsmap

        # XSD version 2.6 does not have clin_shared namespace, which will raise
        # exception when using xpath
        if "clin_shared" not in namespaces:
            namespaces["clin_shared"] = "NA"

        for data_type, params in self.xpath_ref.items():
            # Base properties
            schema = dictionary.schema[data_type]
            clinical = {"type": data_type}
            for values in params:
                for root in self.get_xml_roots(doc_root, values["root"], namespaces):
                    if "generated_id" in values:
                        clinical["id"] = str(
                            uuid5(
                                UUID(values["generated_id"]["namespace"]),
                                self.xpath(
                                    root,
                                    values["generated_id"]["name"],
                                    namespaces,
                                    nullable=False,
                                ),
                            )
                        )

                    if "edges" in values:
                        self.insert_edges(clinical, root, values["edges"], namespaces)

                    if "edges_by_property" in values:
                        self.insert_edges_by_property(
                            clinical, root, values["edges_by_property"], namespaces
                        )

                    props_roots = [root]
                    for path in values.get("additional_property_roots", []):
                        roots = self.get_xml_roots(
                            doc_root, path, namespaces, nullable=True
                        )
                        if roots:
                            props_roots += roots

                    self.insert_properties(
                        clinical, props_roots, values["properties"], namespaces, schema
                    )
            self.docs.append(clinical)

        return self

    def insert_edges(self, doc, root, edges, namespaces):
        for edge_label, edge in edges.items():
            for dst_label, props in edge.items():
                edge_cls = flask.current_app.db.get_edge_by_labels(
                    doc["type"], edge_label, dst_label
                )
                xpath = self.xpath(
                    root=root, namespaces=namespaces, nullable=False, **props
                )
                xpath = xpath.lower()
                # TODO(pyt): the edge dst's id is cast to lowercase as
                # in bcr_xml2json, need to unify the xml2json conversion
                # for clinical and biospec xmls
                doc[edge_cls.__src_dst_assoc__] = {"id": xpath}

    def insert_edges_by_property(self, doc, root, edges, namespaces):
        for edge_label, edge in edges.items():
            for dst_label, dst_property in edge.items():
                edge_cls = flask.current_app.db.get_edge_by_labels(
                    doc["type"], edge_label, dst_label
                )
                xpath = lambda props: self.xpath(
                    root=root, namespaces=namespaces, nullable=False, **props
                )
                doc[edge_cls.__src_dst_assoc__] = {
                    key: xpath(props) for key, props in dst_property.items()
                }

    def insert_properties(self, doc, roots, properties, namespaces, schema):
        for root in roots:
            for key, props in properties.items():
                value = self.xpath(
                    root=root,
                    path=props["path"],
                    namespaces=namespaces,
                    suffix=props.get("suffix", ""),
                )
                _type = props["type"]
                is_nan = isinstance(value, float) and math.isnan(value)
                if value is None or is_nan:
                    if key not in doc:
                        key_type = schema["properties"][key].get("type", [])
                        if "default" in props:
                            doc[key] = props["default"]
                        elif "null" in key_type:
                            doc[key] = None
                    continue
                doc[key] = munge_property(value, _type)


def munge_property(prop, _type):

    types = {
        "int": int,
        "long": int,
        "float": float,
        "str": str,
        "str.lower": lambda x: str(x).lower(),
    }

    if _type == "bool":
        prop = to_bool(prop)
    else:
        prop = types[_type](prop) if prop else prop
    return prop
