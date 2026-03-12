import pytest
from rdflib import Graph, URIRef

from plastron.models.letter import Letter
from plastron.models.newspaper import Issue
from plastron.models.poster import Poster
from plastron.models.umd import Item
from plastron.namespaces import bibo, rdf, umd
from plastron.serializers import detect_resource_class


@pytest.mark.parametrize(
    ('rdf_types', 'expected_class'),
    [
        ([umd.Item], Item),
        ([umd.Item, bibo.Letter], Item),
        ([umd.Item, bibo.Poster], Item),
        ([umd.Issue, bibo.Issue], Issue),
        ([bibo.Letter], Letter),
        ([bibo.Issue], Issue),
        ([bibo.Image], Poster),
    ]
)
def test_detect_resource_class(rdf_types, expected_class):
    graph = Graph()
    for rdf_type in rdf_types:
        graph.add((URIRef(''), rdf.type, rdf_type))

    assert detect_resource_class(graph, URIRef('')) is expected_class


def test_detect_resource_class_fallback():
    graph = Graph()
    assert detect_resource_class(graph, URIRef(''), fallback=Item) is Item


def test_detect_resource_class_no_fallback():
    graph = Graph()
    with pytest.raises(RuntimeError):
        detect_resource_class(graph, URIRef(''))
