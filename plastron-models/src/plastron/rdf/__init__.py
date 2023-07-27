from argparse import ArgumentTypeError
from typing import Tuple

from rdflib import URIRef, Literal
from rdflib.util import from_n3

from plastron import namespaces
from plastron.namespaces import dcterms


def parse_predicate_list(string, delimiter=','):
    if string is None:
        return None
    manager = namespaces.get_manager()
    return [from_n3(p, nsm=manager) for p in string.split(delimiter)]


def parse_data_property(p: str, o: str) -> Tuple[URIRef, Literal]:
    return from_n3(p, nsm=namespaces.get_manager()), Literal(from_n3(o))


def parse_object_property(p: str, o: str) -> Tuple[URIRef, URIRef]:
    predicate = from_n3(p, nsm=namespaces.get_manager())
    obj = uri_or_curie(o)
    return predicate, obj


def uri_or_curie(arg: str) -> URIRef:
    if arg and (arg.startswith('http://') or arg.startswith('https://')):
        # looks like an absolute HTTP URI
        return URIRef(arg)
    try:
        term = from_n3(arg, nsm=namespaces.get_manager())
    except KeyError:
        raise ArgumentTypeError(f'"{arg[:arg.index(":") + 1]}" is not a known prefix')
    if not isinstance(term, URIRef):
        raise ArgumentTypeError(f'"{arg}" must be a URI or CURIE')
    return term


def get_title_string(graph, separator='; '):
    return separator.join([t for t in graph.objects(predicate=dcterms.title)])
