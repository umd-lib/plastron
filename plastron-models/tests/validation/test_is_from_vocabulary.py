from unittest.mock import MagicMock

import httpretty
import pytest
from rdflib import Graph, URIRef, Literal

import plastron.validation
from plastron.namespaces import rdfs
from plastron.validation.vocabularies import Vocabulary


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


@pytest.mark.parametrize(
    ('vocab_uri', 'term_count', 'valid_term', 'invalid_term'),
    [
        ('http://vocab.lib.umd.edu/termsOfUse#', 1, 'test', 'INVALID'),
        ('http://vocab.lib.umd.edu/set#', 1, 'test', 'INVALID'),
        ('http://vocab.lib.umd.edu/form#', 55, 'photographs', 'INVALID'),
        ('http://vocab.lib.umd.edu/rightsStatement#', 8, 'InC', 'INVALID'),
        ('http://vocab.lib.umd.edu/collection#', 1376, '0001-GDOC', 'INVALID'),
        ('http://purl.org/dc/dcmitype/', 12, 'Image', 'INVALID'),
    ]
)
def test_vocabulary(vocab_uri, term_count, valid_term, invalid_term):
    vocab = Vocabulary(vocab_uri)
    assert len(vocab) == term_count
    assert valid_term in vocab
    assert URIRef(vocab_uri + valid_term) in vocab
    assert invalid_term not in vocab


def test_is_from_vocabulary_with_updates_via_mock(monkeypatch, empty_graph, one_subject_graph):
    mock_get_vocabulary = MagicMock()
    mock_get_vocabulary.side_effect = [empty_graph, one_subject_graph]
    monkeypatch.setattr(plastron.validation.vocabularies, 'get_vocabulary_graph', mock_get_vocabulary)
    vocab = Vocabulary('http://example.com/vocab#')
    term = URIRef('http://example.com/vocab#term')
    # this first call should fail, the second should succeed
    # because get_vocabulary_graph is called twice
    assert term not in vocab
    assert term in vocab
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
    vocab = Vocabulary('http://example.com/vocab#')
    term = URIRef('http://example.com/vocab#term')
    # this first call should fail, the second should succeed
    # because two HTTP requests are made with different responses
    assert term not in vocab
    assert term in vocab
