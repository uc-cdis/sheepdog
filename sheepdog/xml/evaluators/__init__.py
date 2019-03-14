import math
from abc import ABCMeta, abstractmethod

PROPERTY_TYPES = {
    'int': int,
    'long': long,
    'float': float,
    'str': str,
    'str.lower': lambda x: str(x).lower(),
    'str.title': lambda x: str(x).title(),
}


class Evaluator(object):
    """
        Abstract class for evaluating node attributes based on their paths in the xml. The evaluator to use will
        be specified in the xml mappings found in gdcdatamodels, if non is specified, the default BasicEvaluator
        will be used
    """

    __metaclass__ = ABCMeta

    def __init__(self, root, namespaces, mappings):
        """
        Args:
            root (lxml.etree._Element): The root element based on which the property path should search
            namespaces (dict): dictionary of all possible namespaces found in the xml
            mappings (dict): mappings for the current property as found in the xml mappings. Mappings hold things like
                evaluator to use, and the path to search
        """
        self.root_element = root
        self.property_mappings = mappings
        self.xml_namespaces = namespaces

    @property
    def path(self):
        return self.property_mappings.get("path")

    @property
    def is_nullable(self):
        return self.property_mappings.get("nullable") in [None, "true", "t", "yes"]

    @property
    def suffix(self):
        return self.property_mappings.get("suffix", "")

    @property
    def data_type(self):
        return self.property_mappings.get("type", "str")

    @property
    def default(self):
        return self.property_mappings.get("default")

    def get_evaluator_property(self, key):
        return self.property_mappings.get("evaluator").get(key)

    @abstractmethod
    def _evaluate(self):
        """ Property specific complex evaluation logic is implemented here
        Returns:
             Any
        """
        raise NotImplementedError("Not Implemented")

    def evaluate(self):
        """Simple wrapper that calls evaluate and does type conversion
            Returns:
                Any: Value on the specified XML element
            Raises:
                Exception: if value cannot be deciphered and field is not nullable
        """
        val = self._evaluate()

        # check defaults
        if val is None and self.default:
            val = self.default

        # check nullable
        if val is None and not self.is_nullable:
            raise ValueError("Can't find element {}".format(self.path))

        # check if value mapping is required and map to appropriate value
        val = self.map_values(val)

        # check if suffix can be appended
        if self.suffix and self.data_type == "str":
            return str(val) + self.suffix

        return self.evaluate_type(val)

    def evaluate_type(self, value):

        if value is None:
            return None

        # handle NaN
        if self.data_type == "float" and math.isnan(float(value)):
            return None

        prop = self._to_bool(value) if self.data_type == "bool" else PROPERTY_TYPES[self.data_type](value)
        return prop

    def search_path(self, path=None, nullable=True):

        paths = path or self.path
        if isinstance(paths, str):
            return self.root_element.xpath(paths, namespaces=self.xml_namespaces, nullable=nullable)
        raise ValueError("{} property must be a string, but {} found".format(self.__class__.__name__, paths))

    @staticmethod
    def _to_bool(val):
        possible_true_values = ['true', 'yes', 't']
        possible_false_values = ['false', 'no', 'f']

        if val is None:
            return None
        if val.lower() in possible_true_values:
            return True
        elif val.lower() in possible_false_values:
            return False
        else:
            raise ValueError("Cannot convert {} to boolean".format(val))

    def map_values(self, value):
        """ Checks if field has list of possible values that map to a specific value """
        value_mappings = self.property_mappings.get("values")  # {value: [possible value list]}
        if not value_mappings:
            return value

        if isinstance(value, str):
            # enforce lower case comparison, mapping values are lower case
            value = value.lower()

        for val, possible_values in value_mappings.items():
            if value in possible_values:
                return val
        raise ValueError("XML value {} not in the mapping: {}".format(value, value_mappings))
