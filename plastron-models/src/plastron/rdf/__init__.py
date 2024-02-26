from argparse import ArgumentTypeError
from typing import Tuple, Optional, List

from rdflib import URIRef, Literal
from rdflib.term import Node
from rdflib.util import from_n3

from plastron import namespaces
from plastron.namespaces import dcterms


def parse_predicate_list(string: str, delimiter: str = ',') -> Optional[List[Node]]:
    if string is None:
        return None
    manager = namespaces.get_manager()
    return [from_n3(p, nsm=manager) for p in string.split(delimiter)]


def parse_data_property(p: str, o: str) -> Tuple[URIRef, Literal]:
    """Convert a pair of strings to a URIRef predicate and Literal object."""
    return uri_or_curie(p), Literal(from_n3(o))


def parse_object_property(p: str, o: str) -> Tuple[URIRef, URIRef]:
    """Convert a pair of strings to a URIRef predicate and URIRef object."""
    return uri_or_curie(p), uri_or_curie(o)


def uri_or_curie(arg: str) -> URIRef:
    """Convert a string to a URIRef. If it begins with either `http://`
    or `https://`, treat it as an absolute HTTP URI. Otherwise, try to
    parse it as a CURIE (e.g., "dcterms:title") and return the expanded
    URI. If the prefix is not recognized, or if `from_n3()` returns anything
    but a URIRef, raises `ArgumentTypeError`."""
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
