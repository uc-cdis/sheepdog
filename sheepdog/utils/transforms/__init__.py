"""
TODO
"""

import csv
import io

from flask import current_app
from psqlgraph import Node

from sheepdog.errors import UserError
from sheepdog.utils.transforms.bcr_xml_to_json import (
    BcrXmlToJsonParser,
    BcrClinicalXmlToJsonParser,
)
from sheepdog.globals import SUB_DELIMITERS


def parse_bool_from_string(value):
    """
    Return a boolean given a string value *iff* :param:` value` is a valid
    string representation of a boolean, otherwise return the original
    :param: `value` to be handled by later type checking.

    ..note:
        ``bool('maybe') is True``, this is undesirable, but
        ``parse_bool_from_string('maybe') == 'maybe'``
    """
    mapping = {"true": True, "false": False}
    return mapping.get(strip(value).lower(), value)


def parse_list_from_string(value):
    """
    Handle array fields by converting them to a list.
    Try to cast to float to handle arrays of numbers.
    Example:
        a,b,c -> ['a','b','c']
        1,2,3 -> [1,2,3]
    """
    items = [x.strip() for x in value.split(",")]
    try:
        items = [float(x) for x in items]
    except ValueError:
        pass  # not an array of numbers
    return items


def set_row_type(row):
    """Get the class for a row dict, setting 'type'. Coerce '' -> None."""
    row["type"] = row.get("type", None)
    if not row["type"]:
        row.pop("type")
        return None
    return Node.get_subclass(row["type"])


def strip(text):
    """
    Strip if the text is a string
    """

    if not isinstance(text, str):
        return text

    else:
        return text.strip()


def strip_whitespace_from_str_dict(dictionary):
    """
    Return new dict with leading/trailing whitespace removed from keys and
    values.
    """
    return {strip(key): strip(value) for key, value in dictionary.items()}


def get_links_from_row(row):
    """Return a dict of key/value pairs that are links."""
    return {k: v for k, v in row.items() if "." in k}


def get_props_from_row(row):
    """Return a dict of key/value pairs that are props, not links."""
    return {k: v for k, v in row.items() if "." not in k}


class DelimitedConverter(object):
    """
    TODO
    """

    def __init__(self):
        self.reader = csv.reader(io.StringIO(""))
        self.format = ""
        self.errors = []
        self.docs = []

    def set_reader(self, _):
        """
        Implement this in a subclass to self.reader to be an iterable of rows
        given a doc.
        """
        msg = "set_reader generator not implemented for {}".format(type(self))
        raise NotImplementedError(msg)

    def convert(self, doc):
        """
        Add an entire document to the converter. Return docs and errors
        gathered so far.
        """
        try:
            self.set_reader(doc)
            list(map(self.add_row, self.reader))
        except Exception as e:
            current_app.logger.exception(e)
            raise UserError("Unable to parse document")
        return self.docs, self.errors

    @staticmethod
    def get_unknown_cls_dict(row):
        """
        TODO: What?

        Either none or invalid type, don't know what properties
        to expect, property types, etc, so add a doc with
        non-link properties (i.e. to allow identification the
        error report later) and short circuit.
        """
        return get_props_from_row(row)

    def add_row(self, row):
        """
        Add a canonical JSON entity for given a :param: `row`.

        Args:
            row (dict): column, value for a given row in delimited file

        Return:
            None
        """
        doc, links = {}, {}
        row = strip_whitespace_from_str_dict(row)

        # Remove asterisks from dict keys
        for key in list(row):
            row[key.lstrip("*")] = row.pop(key)

        # Parse type
        cls = set_row_type(row)
        if cls is None:
            return

        # Add properties
        props_dict = get_props_from_row(row)
        for key, value in props_dict.items():
            # Translating a tsv null value (empty string) to None so the dictionary can remove that key's value
            if value == "":
                doc[key] = None
            elif value == "null":
                doc[key] = None
            else:
                converted = self.convert_type(cls, key, value)
                if converted is not None:
                    doc[key] = converted

        # Add links
        links_dict = get_links_from_row(row)
        for key, value in links_dict.items():
            self.add_link_value(links, cls, key, value)
        doc.update(links)
        self.docs.append(doc)

    def add_link_value(self, links, cls, key, value):
        key_parts = key.split(".")
        if len(key_parts) == 0:
            return

        link_name = key_parts[0]
        if not link_name:
            error = "Invalid link name: {}".format(key)
            return self.record_error(error, columns=[key])
        prop = ".".join(key_parts[1:])
        if not prop:
            error = "Invalid link property name: {}".format(key)
            return self.record_error(error, columns=[key])

        if link_name not in links:
            links[link_name] = []

        l_values = self.value_to_list_value(cls, link_name, prop, value)
        if l_values is not None:
            links[link_name].extend(l_values)

    def value_to_list_value(self, cls, link_name, prop, value):
        if value is None:
            return value
        l_values = value.split(SUB_DELIMITERS.get(self.format))
        r_values = []
        for v in l_values:
            converted_value = self.convert_link_value(cls, link_name, prop, v)
            # only add the prop if there is a link - for example,
            # TSV submissions may include empty link columns
            if converted_value:
                r_values.append({prop: converted_value})
        return r_values

    @staticmethod
    def get_converted_type_from_list(cls, prop_name, value):
        types = cls.__pg_properties__.get(prop_name, (str,))
        value_type = types[0]
        try:
            if value_type == bool:
                return parse_bool_from_string(value)
            elif value_type == list:
                return parse_list_from_string(value)
            elif value_type == float:
                if float(value).is_integer():
                    return int(float(value))
                else:
                    return float(value)
            elif strip(value) == "":
                return None
            else:
                return value_type(value)
        except Exception as exception:  # pylint: disable=broad-except
            current_app.logger.exception(exception)
            return value

    @staticmethod
    def convert_type(to_cls, key, value):
        """
        Cast value based on key.
        Args:
            to_cls: class to be converted
            key: property is converted
            value: value is concerted

        Return:
            value with correct type
        """
        if value is None:
            return None

        key, value = strip(key), strip(value)
        return DelimitedConverter.get_converted_type_from_list(to_cls, key, value)

    @staticmethod
    def convert_link_value(to_cls, link_name, prop, value):
        """
        Cast value based on key.
        Args:
            to_cls: class to be converted
            link_name: name of link to be converted
            key: property is converted
            value: value is concerted

        Return:
            value with correct type
        """
        if value is None or value in ["", "null"]:
            return None

        link_name, value = strip(link_name), strip(value)
        edge = getattr(to_cls, link_name)
        return DelimitedConverter.get_converted_type_from_list(
            Node.get_subclass_named(edge.target_class.__dst_class__), prop, value
        )

    @property
    def is_valid(self):
        """Indicate that the conversion is valid if there are no errors."""
        return not self.errors

    def record_error(self, message, columns=None, **kwargs):
        """
        Record an error message.

        Args:
            message (str): error message to record
            keys (list): which keys in the JSON were invalid

        Return:
            None
        """
        if columns is None:
            columns = []
        self.errors.append(
            dict(message=message, columns=columns, line=self.reader.line_num, **kwargs)
        )


class TSVToJSONConverter(DelimitedConverter):
    def set_reader(self, doc):
        # Standardize the new line format
        self.format = "tsv"
        doc = "\n".join(strip(doc).splitlines())
        f = io.StringIO(doc)
        self.reader = csv.DictReader(f, delimiter="\t")


class CSVToJSONConverter(DelimitedConverter):
    def set_reader(self, doc):
        # Standardize the new line format
        self.format = "csv"
        doc = "\n".join(strip(doc).splitlines())
        f = io.StringIO(doc)
        self.reader = csv.DictReader(f, delimiter=",")
