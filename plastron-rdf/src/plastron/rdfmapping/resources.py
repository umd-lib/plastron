from collections import defaultdict
from copy import deepcopy, copy
from typing import List, Optional, Union, Any, Type, TypeVar
from uuid import uuid4

from rdflib import Graph, URIRef

from plastron.rdfmapping.descriptors import ObjectProperty, Property, DataProperty
from plastron.rdfmapping.graph import TrackChangesGraph
from plastron.rdfmapping.properties import RDFProperty
from plastron.rdfmapping.validation import ValidationResultsDict


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
    rdf_property_names = set()
    default_values = defaultdict(set)
    validators = []

    def __init_subclass__(cls, **kwargs):
        # make new copies of the class variables for the subclasses
        # at this point, the Property descriptors' __set_name__ methods
        # have already run, so we retroactively add the names of
        # this class's Property descriptors
        own_properties = {k for k, v in cls.__dict__.items() if isinstance(v, Property)}
        cls.rdf_property_names = copy(cls.__base__.rdf_property_names) | own_properties
        # default_values and validators are modified by decorators, which
        # run after the __init_subclass__ method
        cls.default_values = deepcopy(cls.__base__.default_values)
        cls.validators = deepcopy(cls.__base__.validators)

    def __init__(self, uri: Union[URIRef, str] = None, graph: Optional[Graph] = None, **kwargs):
        if uri is not None:
            self._uri = URIRef(uri)
        else:
            # use a unique UUID URI for resources that do not (yet) have a URI subject
            self._uri = URIRef(uuid4().urn)

        if graph:
            if isinstance(graph, TrackChangesGraph):
                self._graph = graph
            else:
                self._graph = TrackChangesGraph()
                for triple in graph:
                    self._graph.add(triple)
                self._graph.apply_changes()
        else:
            self._graph = TrackChangesGraph()
            self.set_properties(**self.default_values)
        self.set_properties(**kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.apply_changes()

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
        fragment = object_class(uri=uri, graph=self._graph)
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
    def graph(self) -> TrackChangesGraph:
        return self._graph

    @property
    def has_changes(self) -> bool:
        return self._graph.has_changes

    def apply_changes(self):
        self._graph.apply_changes()

    @property
    def uri(self) -> URIRef:
        """The primary subject for this RDF resource."""
        return self._uri

    @uri.setter
    def uri(self, new_uri: URIRef):
        self._graph.change_uri(self._uri, new_uri)
        self._uri = new_uri

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


RDFResourceType = TypeVar('RDFResourceType', bound=RDFResourceBase)
