import plastron.validation.rules
import sys
from copy import copy
from rdflib import Graph, RDF, URIRef, Literal
from plastron.namespaces import rdf
from plastron.validation import ResourceValidationResult

# alias the rdflib Namespace
ns = rdf


def init_class_attr(cls, name, default):
    if hasattr(cls, name):
        # there's a attribute set somewhere in the inheritance hierarchy
        # copy it as the basis for this class's instance of that attribute
        setattr(cls, name, copy(getattr(cls, name)))
    else:
        setattr(cls, name, default)


def rdf_class(*types):
    def add_types(cls):
        cls.rdf_types.update(types)
        return cls
    return add_types


# metaclass that makes copies of specific class attributes
# as we go down the inheritance hierarchy
class Meta(type):
    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)
        # name and URI to property type class lookups
        init_class_attr(cls, 'name_to_prop', {})
        init_class_attr(cls, 'uri_to_prop', {})
        # list of property type classes for this class
        init_class_attr(cls, 'prop_types', [])
        # RDF types
        init_class_attr(cls, 'rdf_types', set())
        return cls


class RDFProperty(object):
    uri = None

    def __init__(self):
        self.values = []

    def append(self, other):
        self.values.append(other)

    def update(self, new_values):
        # take the set differences to find deleted and inserted values
        old_values_set = set(self.values)
        new_values_set = set(new_values)
        self.values = new_values
        deleted_values = old_values_set - new_values_set
        inserted_values = new_values_set - old_values_set
        # return the sets so the caller could construct a SPARQL update
        return deleted_values, inserted_values

    def __str__(self):
        return ' '.join(map(str, self.values))

    def __iter__(self):
        return iter(self.values)

    def __len__(self):
        return len(self.values)

    def __getattr__(self, item):
        if len(self.values) == 0:
            return None
        elif len(self.values) > 1:
            raise AttributeError(f'Multiple values for attribute {item} of {self}')
        else:
            return getattr(self.values[0], item)

    def __getitem__(self, item):
        return self.values[item]

    def triples(self, subject):
        for value in self.values:
            if value is not None:
                yield (subject, self.uri, self.get_term(value))


class RDFDataProperty(RDFProperty):
    @classmethod
    def get_term(cls, value):
        return value if isinstance(value, Literal) else Literal(value)


class RDFObjectProperty(RDFProperty):
    is_embedded = False
    obj_class = None

    def __getitem__(self, item):
        if isinstance(item, URIRef):
            for obj in self.values:
                if obj.uri == item:
                    return obj
        raise IndexError(f'Cannot find object by URI {item}')

    def append_new(self, **kwargs):
        obj = self.obj_class(**kwargs)
        self.append(obj)

    @classmethod
    def get_term(cls, value):
        if hasattr(value, 'uri'):
            return URIRef(value.uri)
        elif isinstance(value, URIRef):
            return value
        else:
            raise ValueError('Expecting a URIRef or an object with a uri attribute')


def data_property(name, uri):
    def add_property(cls):
        type_name = f'{cls.__name__}.{name}'
        prop_type = type(type_name, (RDFDataProperty,), {
            'name': name,
            'uri': uri
        })
        cls.name_to_prop[name] = prop_type
        cls.uri_to_prop[uri] = prop_type
        cls.prop_types.append(prop_type)
        return cls

    return add_property


class Resource(metaclass=Meta):
    @classmethod
    def from_graph(cls, graph, subject=''):
        return cls(uri=subject).read(graph)

    def __init__(self, uri=None, **kwargs):
        if uri is None:
            uri = ''
        self.uri = URIRef(uri)
        self.props = {}
        self.unmapped_triples = []

        for prop_type in self.prop_types:
            prop = prop_type()
            self.props[prop.name] = prop

        for key, value in kwargs.items():
            setattr(self, key, value)

    def read(self, graph):
        for (s, p, o) in graph.triples((self.uri, None, None)):
            if p in self.uri_to_prop:
                prop_type = self.uri_to_prop[p]
                if issubclass(prop_type, RDFObjectProperty) and prop_type.obj_class is not None:
                    obj = prop_type.obj_class(uri=o)
                    # recursively read embedded objects whose triples should be part of the same graph
                    if prop_type.is_embedded:
                        obj.read(graph)
                else:
                    obj = o
                getattr(self, prop_type.name).append(obj)
            else:
                self.unmapped_triples.append((s, p, o))
        return self

    def __getattr__(self, name):
        if name in self.name_to_prop:
            return self.props[name]
        else:
            raise AttributeError(f"No predicate mapped to {name}")

    def __setattr__(self, name, value):
        if name in self.name_to_prop:
            if not isinstance(value, list):
                value = [value]
            # TODO: wrap these values in rdflib classes?
            self.props[name].values = value
        else:
            # attribute names that aren't mapped to RDF properties
            # just get set like normal attributes
            super().__setattr__(name, value)

    def properties(self):
        return [prop for prop in self.props.values()]

    def data_properties(self):
        return [prop for prop in self.props.values() if isinstance(prop,
                                                                   RDFDataProperty)]

    def object_properties(self):
        return [prop for prop in self.props.values() if isinstance(prop,
                                                                   RDFObjectProperty)]

    def embedded_objects(self):
        for prop in [prop for prop in self.object_properties() if prop.is_embedded]:
            for v in prop.values:
                # recursively expand embedded objects
                if hasattr(v, 'embedded_objects'):
                    for vo in v.embedded_objects():
                        yield vo
                yield v

    def linked_objects(self):
        for prop in [prop for prop in self.object_properties() if not prop.is_embedded]:
            for v in [v for v in prop.values if hasattr(v, 'uri')]:
                yield v

    def graph(self, nsm=None):
        subject = URIRef(self.uri)
        graph = Graph(namespace_manager=nsm)
        for rdf_type in self.rdf_types:
            graph.add((subject, RDF.type, rdf_type))

        for prop in self.properties():
            for (s, p, o) in prop.triples(self.uri):
                graph.add((s, p, o))

        # add graphs for all objects that should be embedded in the graph
        # for this resource; typically, these are resources that are have
        # hash URI identifiers
        for obj in self.embedded_objects():
            if obj is not None:
                graph = graph + obj.graph()

        # any triples that were loaded from the source graph but that aren't
        # mapped to specific Python attributes
        for (s, p, o) in self.unmapped_triples:
            graph.add((subject, p, o))

        return graph

    def print(self, format='turtle', file=sys.stdout, nsm=None):
        print(self.graph(nsm=nsm).serialize(format=format).decode(), file=file)

    def validate(self, ruleset):
        result = ResourceValidationResult(self)
        for field, rules in ruleset.items():
            for rule_name, arg in rules.items():
                rule = getattr(plastron.validation.rules, rule_name)
                prop = getattr(self, field)
                if rule(prop, arg):
                    result.passes(prop, rule, arg)
                else:
                    result.fails(prop, rule, arg)
        return result


def object_property(name, uri, embed=False, obj_class=None):
    def add_property(cls):
        type_name = f'{cls.__name__}.{name}'
        prop_type = type(type_name, (RDFObjectProperty,), {
            'name': name,
            'uri': uri,
            'is_embedded': embed,
            'obj_class': obj_class
        })
        cls.name_to_prop[name] = prop_type
        cls.uri_to_prop[uri] = prop_type
        cls.prop_types.append(prop_type)
        return cls
    return add_property
