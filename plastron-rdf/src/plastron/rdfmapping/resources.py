from collections import defaultdict
from copy import deepcopy, copy
from typing import List, Optional, Union, Any, Type, Dict
from uuid import uuid4

from rdflib import Graph, URIRef
from rdflib.term import BNode

from plastron.rdfmapping.descriptors import ObjectProperty, Property, DataProperty
from plastron.rdfmapping.properties import RDFProperty, ValidationResult, ValidationResultsDict


def is_iterable(value: Any) -> bool:
    try:
        iter(value)
    except TypeError:
        # this is non-iterable
        return False
    else:
        # special case for strings; they are technically iterable,
        # but in all but the most specialized cases we want to treat
        # them as single values
        return not isinstance(value, str)


class RDFResourceBase:
    rdf_property_names = []
    default_values = defaultdict(set)
    validators = []

    def __init_subclass__(cls, **kwargs):
        # make new copies of the class variables for the subclasses
        # at this point, the Property descriptors' __set_name__ methods
        # have already run, so we retroactively add the names of
        # this class's Property descriptors
        own_properties = [k for k, v in cls.__dict__.items() if isinstance(v, Property)]
        cls.rdf_property_names = copy(cls.__base__.rdf_property_names) + own_properties
        # default_values and validators are modified by decorators, which
        # run after the __init_subclass__ method
        cls.default_values = copy(cls.__base__.default_values)
        cls.validators = copy(cls.__base__.validators)

    def __init__(self, uri: Union[URIRef, str] = None, graph: Optional[Graph] = None, **kwargs):
        if uri is not None:
            self._uri = URIRef(uri)
        else:
            self._uri = BNode()
        self.base_graph = graph if graph is not None else Graph()

        self.inserts = set()
        self.deletes = set()
        if not graph:
            self.set_properties(**self.default_values)
        self.set_properties(**kwargs)

    def get_fragment_resource(
            self,
            object_class: Type['RDFResourceBase'],
            fragment_id: Optional[str] = None,
    ) -> 'RDFResourceBase':
        """
        Embedded (i.e., "fragment") resources share a graph with their parent resource. They
        are essentially a different "filter" through which to view the same graph.

        :param fragment_id: fragment identifier to append to the parent resource's URI. Defaults to a new UUIDv4 string.
        :param object_class:
        :return:
        """
        if fragment_id is None:
            fragment_id = str(uuid4())
        uri = URIRef(self.uri + '#' + fragment_id)
        fragment = object_class(uri=uri, graph=self.base_graph)
        fragment.inserts = self.inserts
        fragment.deletes = self.deletes
        return fragment

    def set_properties(self, **kwargs):
        for name, value in kwargs.items():
            if name not in self.rdf_property_names:
                raise ValueError(f'Unknown property name: {name}')
            if is_iterable(value):
                # value is iterable, add each value separately
                prop = getattr(self, name)
                prop.clear()
                for v in value:
                    prop.add(v)
            else:
                # this is non-iterable, set a single value
                setattr(self, name, value)

        self.apply_changes()

    @property
    def graph(self) -> Graph:
        graph = deepcopy(self.base_graph)
        for triple in self.deletes:
            graph.remove(triple)
        for triple in self.inserts:
            graph.add(triple)
        return graph

    @property
    def has_changes(self) -> bool:
        return len(self.deletes) > 0 or len(self.inserts) > 0

    def apply_changes(self):
        for triple in self.deletes:
            self.base_graph.remove(triple)
        for triple in self.inserts:
            self.base_graph.add(triple)
        self.clear_changes()

    def clear_changes(self):
        self.deletes.clear()
        self.inserts.clear()

    @property
    def uri(self) -> URIRef:
        return self._uri

    def rdf_properties(self) -> List[RDFProperty]:
        return [getattr(self, attr_name) for attr_name in self.rdf_property_names]

    @property
    def is_valid(self) -> bool:
        if not all(p.is_valid for p in self.rdf_properties()):
            return False
        if not all(test(self) for test in self.validators):
            return False
        return True

    def validate(self) -> ValidationResultsDict:
        results = ValidationResultsDict({name: getattr(self, name).is_valid for name in self.rdf_property_names})
        for test in self.validators:
            results['_' + test.__name__] = test(self)
        return results


class RDFResource(RDFResourceBase):
    rdf_type = ObjectProperty(URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), repeatable=True)
    label = DataProperty(URIRef('http://www.w3.org/2000/01/rdf-schema#label'), repeatable=True)


class ResourceError(Exception):
    pass
