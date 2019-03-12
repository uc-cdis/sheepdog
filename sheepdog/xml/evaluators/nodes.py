import copy
from uuid import uuid5, UUID

from gdcdatamodel import models

from sheepdog.xml.evaluators import Evaluator
from sheepdog.xml.evaluators.fields import BasicEvaluator, FilterElementEvaluator, LastFollowUpEvaluator, \
    TreatmentTherapyEvaluator, VitalStatusEvaluator


class NodeEvaluator(Evaluator):
    """Simple composite that evaluates all properties and edges of a node"""

    def __init__(self, node_type, root, mappings, namespaces):
        """ Evaluates all properties for a given node
        Args:
            node_type (str):
            root (lxml.etree._Element):
            mappings (dict): property mappings from yaml definition
            namespaces (dict): xml namespaces
        """

        super(NodeEvaluator, self).__init__(root, namespaces, mappings)
        self.data = dict(type=node_type)

    def set_property(self, key, value):
        self.data[key] = value

    def set_edge(self, edge_label, dst_label, value, property_name="id"):
        value = value.lower() if property_name == "id" else value
        edge_cls = get_psqlgraph_edge_by_label(self.node_type, edge_label, dst_label)
        self.data[edge_cls.__src_dst_assoc__] = {property_name: value}

    @property
    def node_type(self):
        return self.data["type"]

    def evaluate_uuid(self):
        id_params = self.property_mappings.get("generated_id")
        if id_params:
            _id = self.search_path(id_params["name"], nullable=False)
            _id = _id[0].text if _id else _id
            self.data["id"] = str(uuid5(UUID(id_params['namespace']), _id))

    def evaluate_edges(self):
        for edge_label, edge in self.property_mappings.get("edges", {}).items():
            for dst_label, props in edge.items():
                self.set_edge(edge_label, dst_label, self._evaluate_property(props))

    def evaluate_edge_by_properties(self):
        for edge_label, edge in self.property_mappings.get("edges_by_property", {}).items():
            for dst_label, props in edge.items():

                for key, prop in props.items():
                    self.set_edge(edge_label, dst_label, self._evaluate_property(prop), property_name=key)

    def evaluate_properties(self):

        for key, props in self.property_mappings["properties"].items():
            self.set_property(key, self._evaluate_property(props))

    def _evaluate_property(self, props):
        evaluator = EvaluatorFactory.get_instance(self.root_element, self.xml_namespaces, props)
        return evaluator.evaluate()

    def _evaluate(self):

        self.evaluate_uuid()
        self.evaluate_edges()
        self.evaluate_properties()

        return self.get_data()

    def evaluate(self):
        return self._evaluate()

    def get_data(self):
        return [self.data]


class TreatmentNodeEvaluator(NodeEvaluator):

    def _evaluate(self):

        self.evaluate_edges()
        self.evaluate_properties()

        return self.get_data()

    def get_data(self):
        new_data = []
        for key, data in self.data.items():
            if not isinstance(data, list):
                continue

            for i, entry in enumerate(data):
                treatment_data = copy.deepcopy(self.data)

                # remove key, since its currently a list and not required for the final clinical json data
                treatment_data.pop(key)
                treatment_data.update(entry)
                treatment_data["id"] = self.generate_id(i)
                new_data.append(treatment_data)

        return new_data

    def generate_id(self, index):
        id_params = self.property_mappings.get("generated_id")
        _id = self.search_path(id_params["name"], nullable=False)
        _id = _id[0].text if _id else _id
        return str(uuid5(UUID(id_params['namespace']), _id + str(index)))


class EvaluatorFactory(object):

    @staticmethod
    def get_instance(root, namespaces, props):
        evaluator = props.get("evaluator", {}).get("name")

        if not evaluator:
            # generic evaluator
            return BasicEvaluator(root, namespaces, props)

        if evaluator == "last_follow_up":
            return LastFollowUpEvaluator(root, namespaces, props)

        if evaluator == "vital_status":
            return VitalStatusEvaluator(root, namespaces, props)

        if evaluator == "filter":
            return FilterElementEvaluator(root, namespaces, props)

        if evaluator == "treatment_therapy":
            return TreatmentTherapyEvaluator(root, namespaces, props)

    @staticmethod
    def evaluate_node(data_type, root, values, namespaces):

        if data_type == "treatment":
            evaluator = TreatmentNodeEvaluator(data_type, root, values, namespaces)
        else:
            evaluator = NodeEvaluator(data_type, root, values, namespaces)
        return evaluator.evaluate()


def get_psqlgraph_edge_by_label(src_label, edge_label, dst_label):
    src_class = models.Node.get_subclass(src_label)
    if src_label is None:
        raise ValueError('No classes found with src_label {}'.format(src_label))

    dst_class = models.Node.get_subclass(dst_label)
    if dst_label is None:
        raise ValueError('No classes found with dst_label {}'.format(dst_label))

    edges = [edge for edge in models.Edge.get_subclasses()
             if edge.__src_class__ == src_class.__name__
             and edge.__dst_class__ == dst_class.__name__
             and edge.get_label() == edge_label]
    if len(edges) != 1:
        raise ValueError('Expected 1 edge {}-{}->{}, found {}'.format(
            src_label, edge_label, dst_label, len(edges)))
    return edges[0]
