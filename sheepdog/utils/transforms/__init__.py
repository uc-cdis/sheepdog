"""
TODO
"""

from logging import getLogger
import csv
import StringIO

from psqlgraph import Node
from cdislogging import get_logger

from sheepdog.errors import (
    UserError,
)
from sheepdog.utils.transforms.bcr_xml_to_json import (
    BcrBiospecimenXmlToJsonParser,
    BcrClinicalXmlToJsonParser,
)


logger = get_logger(__name__)


def parse_bool_from_string(value):
    """
    Return a boolean given a string value *iff* :param:`value` is a valid
    string representation of a boolean, otherwise return the original
    :param:`value` to be handled by later type checking.

    ..note:
        ``bool('maybe') is True``, this is undesirable, but
        ``parse_bool_from_string('maybe') == 'maybe'``
    """
    mapping = {'true': True, 'false': False}
    return mapping.get(strip(value).lower(), value)


def set_row_type(row):
    """Get the class for a row dict, setting 'type'. Coerce '' -> None."""
    row['type'] = row.get('type', None)
    if not row['type']:
        row.pop('type')
        return None
    return Node.get_subclass(row['type'])


def strip(text):
    """Strip text as unicode (which includes non-ascii whitespace).

    this will cover a case if the value is NoneType
    """

    if not isinstance(text, basestring):
        return text

    elif not isinstance(text, unicode):
        return unicode(text, "UTF-8").strip()

    else:
        return text.strip()


def strip_whitespace_from_str_dict(dictionary):
    """
    Return new dict with leading/trailing whitespace removed from keys and
    values.
    """
    return {strip(key): strip(value) for key, value in dictionary.iteritems()}


def get_links_from_row(row):
    """Return a dict of key/value pairs that are links."""
    return {k: v for k, v in row.iteritems() if '.' in k}


def get_props_from_row(row):
    """Return a dict of key/value pairs that are props, not links."""
    return {k: v for k, v in row.iteritems() if '.' not in k and v != ''}


class DelimitedConverter(object):
    """
    TODO
    """

    def __init__(self, is_gdc=False):
        self.reader = csv.reader(StringIO.StringIO(''))
        self.errors = []
        self.docs = []
        self.is_gdc = is_gdc

    def set_reader(self, _):
        """
        Implement this in a subclass to self.reader to be an iterable of rows
        given a doc.
        """
        msg = 'set_reader generator not implemented for {}'.format(type(self))
        raise NotImplementedError(msg)

    def convert(self, doc):
        """
        Add an entire document to the converter. Return docs and errors
        gathered so far.
        """
        try:
            self.set_reader(doc)
            map(self.add_row, self.reader)
        except Exception as e:
            logger.exception(e)
            raise UserError('Unable to parse document')
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
        Add a canonical JSON entity for given a :param:`row`.

        Args:
            row (dict): column, value for a given row in delimited file

        Return:
            None
        """
        doc, links = {}, {}
        row = strip_whitespace_from_str_dict(row)
        # Parse type
        cls = set_row_type(row)
        if cls is None:
            return self.docs.append(get_props_from_row(row))

        # Add properties
        props_dict = get_props_from_row(row)
        for key, value in props_dict.iteritems():
            if value == 'null':
                doc[key] = None
            else:
                converted = self.convert_type(cls, key, value, self.is_gdc)
                if converted is not None:
                    doc[key] = converted

        # Add links
        links_dict = get_links_from_row(row)
        for key, value in links_dict.iteritems():
            self.add_link_value(links, cls, key, value)

        doc.update({k: v.values() for k, v in links.iteritems()})
        self.docs.append(doc)

    def add_link_value(self, links, cls, key, value):
        """
        TODO
        """
        converted_value = self.convert_type(cls, key, value, self.is_gdc)
        if converted_value is None:
            return
        if value == 'null':
            converted_value = None

        parsed = key.split('.')
        link = parsed[0]
        if not link:
            error = 'Invalid link name: {}'.format(key)
            return self.record_error(error, columns=[key])
        prop = '.'.join(parsed[1:])
        if not prop:
            error = 'Invalid link property name: {}'.format(key)
            return self.record_error(error, columns=[key])

        # Add to doc
        if '#' in prop:
            items = prop.split('#')
            if len(items) > 2:
                error = '# is not allowed in link identitifer'
                return self.record_error(error, columns=[key])
            prop = items[0]
            link_id = items[1]
        else:
            link_id = 1

        if link in links:
            if link_id in links[link]:
                if isinstance(links[link][link_id], dict):
                    links[link][link_id][prop] = converted_value
                else:
                    # this block should never be reached
                    error = 'name collision: name {} specified twice'
                    return self.record_error(
                        error.format(link), columns=[key, link]
                    )
            else:
                links[link][link_id] = {prop: converted_value}

        else:
            links[link] = {link_id: {prop: converted_value}}

    @staticmethod
    def convert_type(to_cls, key, value, is_gdc=False):
        """
        Cast value based on key.
        TODO
        """
        if value is None:
            return None

        # Currently, gdcdatamodel.models.File.__pg_properties__['file_size'] = (<type 'float'>, <type 'int'>, <type 'long'>)
        # Though it needs to be <type 'int'> only as indexd allows only integer size
        # Per Joe, The change of the model will require a full database migration and a maintanance shutdown
        # Below is a sad temporary workaround:
        if is_gdc:
            if key == 'file_size':
                return int(value)

        key, value = strip(key), strip(value)
        types = to_cls.__pg_properties__.get(key, (str,))
        types = types or (str,)
        value_type = types[0]
        try:
            if value_type == bool:
                return parse_bool_from_string(value)
            elif strip(value) == '':
                return None
            else:
                return value_type(value)
        except Exception as exception:  # pylint: disable=broad-except
            logger.exception(exception)
            return value

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
        self.errors.append(dict(
            message=message, columns=columns, line=self.reader.line_num,
            **kwargs
        ))


class TSVToJSONConverter(DelimitedConverter):

    def set_reader(self, doc):
        # Standardize the new line format
        doc = '\n'.join(strip(doc).splitlines())
        f = StringIO.StringIO(doc)
        self.reader = csv.DictReader(f, delimiter='\t')


class CSVToJSONConverter(DelimitedConverter):

    def set_reader(self, doc):
        # Standardize the new line format
        doc = '\n'.join(strip(doc).splitlines())
        f = StringIO.StringIO(doc)
        self.reader = csv.DictReader(f, delimiter=',')
