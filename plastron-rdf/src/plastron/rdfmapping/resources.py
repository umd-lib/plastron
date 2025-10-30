from collections import defaultdict
from copy import deepcopy, copy
from typing import Optional, Any, Type, TypeVar, Callable
from uuid import uuid4

from rdflib import Graph, URIRef

from plastron.rdfmapping.descriptors import ObjectProperty, Property, DataProperty, OBJECT_CLASSES
from plastron.rdfmapping.graph import TrackChangesGraph, copy_triples
from plastron.rdfmapping.properties import RDFProperty
from plastron.rdfmapping.validation import ValidationResultsDict


def is_iterable(value: Any) -> bool:
    """Returns `True` if `value` is iterable, but not a string. While strings
    in Python are technically iterable, there are many cases where we would
    prefer to not treat them as such."""
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
    """Base class for RDF description classes."""
    rdf_property_names: set = set()
    default_values: dict[Any, set] = defaultdict(set)
    validators: list[Callable[['RDFResourceBase'], bool]] = []

    def __init_subclass__(cls, **kwargs):
        # make new copies of the class variables for the subclasses
        # at this point, the Property descriptors' __set_name__ methods
        # have already run, so we start with the names of this class's
        # own Property descriptors
        cls.rdf_property_names = {k for k, v in cls.__dict__.items() if isinstance(v, Property)}
        cls.default_values = defaultdict(set)
        cls.validators = []
        base_classes = list(filter(lambda c: issubclass(c, RDFResourceBase), cls.__mro__))
        for base_class in base_classes:
            cls.rdf_property_names |= copy(base_class.rdf_property_names)
            # default_values and validators are modified by decorators, which
            # run after the __init_subclass__ method
            cls.default_values.update(deepcopy(base_class.default_values))
            cls.validators.extend(deepcopy(base_class.validators))
        # build the lookup table of class name to class
        OBJECT_CLASSES[cls.__name__] = cls

    def __init__(self, uri: URIRef | str = None, graph: Graph = None, **kwargs):
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
                copy_triples(graph, self._graph)
                copy_triples(graph, self._graph.original)
        else:
            self._graph = TrackChangesGraph()
        self.add_properties(**self.default_values)
        self.add_properties(**kwargs)

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

    def _update_properties(self, properties: dict[str, Any], clear_existing=False):
        for name, value in properties.items():
            if name not in self.rdf_property_names:
                raise ValueError(f'Unknown property name: {name}')

            prop = getattr(self, name)
            if clear_existing:
                prop.clear()
            if is_iterable(value):
                # value is iterable, add each value separately
                for v in value:
                    prop.add(v)
            else:
                # this is non-iterable, set a single value
                prop.add(value)

    def set_properties(self, **kwargs):
        """Set the properties on this object. Clears any existing properties."""
        self._update_properties(properties=kwargs, clear_existing=True)

    def add_properties(self, **kwargs):
        """Add the given properties to this object. Does *not* clear existing properties."""
        self._update_properties(properties=kwargs, clear_existing=False)

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

    def rdf_properties(self) -> list[RDFProperty]:
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

    def redescribe(self, model: Type['RDFResourceType']) -> 'RDFResourceType':
        return model(uri=self.uri, graph=self.graph)


class RDFResource(RDFResourceBase):
    rdf_type = ObjectProperty(URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), repeatable=True)
    label = DataProperty(URIRef('http://www.w3.org/2000/01/rdf-schema#label'), repeatable=True)


class ResourceError(Exception):
    pass


RDFResourceType = TypeVar('RDFResourceType', bound=RDFResourceBase)
