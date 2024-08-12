from unittest.mock import MagicMock

import pytest
from rdflib import Graph, URIRef, Literal

import plastron.models.authorities
from plastron.models.authorities import VocabularyTerm
from plastron.namespaces import rdfs, dcterms, rdf, owl


@pytest.fixture
def vocabulary_graph():
    graph = Graph()
    subject = URIRef('http://example.com/vocab#foo')
    graph.add((subject, rdfs.label, Literal('Foo')))
    graph.add((subject, dcterms.description, Literal('A foo thing')))
    graph.add((subject, rdf.value, Literal('foo foo foo')))
    graph.add((subject, rdfs.comment, Literal('test value')))
    graph.add((subject, owl.sameAs, URIRef('http://foo.example.com/#id')))
    return graph


def test_vocabulary_term(monkeypatch, vocabulary_graph):
    mock_get_vocabulary = MagicMock()
    mock_get_vocabulary.return_value = vocabulary_graph
    monkeypatch.setattr(plastron.models.authorities, 'get_vocabulary_graph', mock_get_vocabulary)

    term = VocabularyTerm(uri=URIRef('http://example.com/vocab#foo'))
    assert term.label.value == Literal('Foo')
    assert term.description.value == Literal('A foo thing')
    assert term.value.value == Literal('foo foo foo')
    assert term.comment.value == Literal('test value')
    assert term.same_as.value == URIRef('http://foo.example.com/#id')
