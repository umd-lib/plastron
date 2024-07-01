from unittest.mock import MagicMock

import httpretty
import pytest
from rdflib import Graph, URIRef, Literal

import plastron.validation.vocabularies
from plastron.namespaces import rdfs
from plastron.validation.rules import is_from_vocabulary


@pytest.fixture
def empty_graph() -> Graph:
    return Graph()


@pytest.fixture
def one_subject_graph() -> Graph:
    graph = Graph()
    graph.add((URIRef('http://example.com/vocab#term'), rdfs.label, Literal('Test Term')))
    return graph


def graph_response(graph: Graph, media_type: str = 'text/turtle') -> httpretty.Response:
    return httpretty.Response(
        graph.serialize(format=media_type),
        adding_headers={'Content-Type': media_type},
    )


def test_is_from_vocabulary_doc_string():
    is_valid = is_from_vocabulary('http://example.com/vocab#')
    assert is_valid.__doc__ == 'from vocabulary http://example.com/vocab#'


def test_is_from_vocabulary_with_updates_via_mock(monkeypatch, empty_graph, one_subject_graph):
    mock_get_vocabulary = MagicMock()
    mock_get_vocabulary.side_effect = [empty_graph, one_subject_graph]
    monkeypatch.setattr(plastron.validation.vocabularies, 'get_vocabulary', mock_get_vocabulary)
    is_valid = is_from_vocabulary('http://example.com/vocab#')
    term = URIRef('http://example.com/vocab#term')
    # this first call should fail, the second should succeed
    # because get_vocabulary is called twice
    assert not is_valid(term)
    assert is_valid(term)
    assert mock_get_vocabulary.call_count == 2


@httpretty.activate
def test_is_from_vocabulary_with_updates_via_http(empty_graph, one_subject_graph):
    httpretty.register_uri(
        method=httpretty.GET,
        uri='http://example.com/vocab#',
        responses=[
            graph_response(empty_graph),
            graph_response(one_subject_graph),
        ],
    )
    is_valid = is_from_vocabulary('http://example.com/vocab#')
    term = URIRef('http://example.com/vocab#term')
    # this first call should fail, the second should succeed
    # because two HTTP requests are made with different responses
    assert not is_valid(term)
    assert is_valid(term)
