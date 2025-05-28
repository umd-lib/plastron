from unittest.mock import MagicMock

import pytest
from rdflib import Graph, URIRef, Literal

import plastron.validation.vocabularies
from plastron.namespaces import rdfs, dcterms, rdf, owl
from plastron.validation.vocabularies import Vocabulary, VocabularyTerm


@pytest.fixture
def vocab():
    return Vocabulary('http://purl.org/dc/dcmitype/')


def test_vocabulary(vocab):
    assert str(vocab) == 'http://purl.org/dc/dcmitype/'
    assert len(vocab) == 12
    terms = list(vocab)
    assert len(terms) == 12
    assert len(vocab.items()) == 12


def test_get_item(vocab):
    assert vocab['Image']


def test_get_item_not_exist(vocab):
    with pytest.raises(KeyError):
        _ = vocab['FAKE_TERM']


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
    monkeypatch.setattr(plastron.validation.vocabularies, 'get_vocabulary_graph', mock_get_vocabulary)

    vocabulary = Vocabulary('http://example.com/vocab#')
    term_class = VocabularyTerm.from_vocab(vocabulary)

    term = term_class(uri=URIRef('http://example.com/vocab#foo'))
    assert term.label.value == Literal('Foo')
    assert term.description.value == Literal('A foo thing')
    assert term.value.value == Literal('foo foo foo')
    assert term.comment.value == Literal('test value')
    assert term.same_as.value == URIRef('http://foo.example.com/#id')


@pytest.mark.parametrize(
    ('vocab_uri', 'name_param', 'expected_name'),
    [
        ('http://example.com/things#', None, 'ThingsVocabularyTerm'),
        ('http://example.com/things/', None, 'ThingsVocabularyTerm'),
        ('http://example.com/things#', 'NewClassName', 'NewClassName'),
    ]
)
def test_vocabulary_term_from_vocab(vocab_uri, name_param, expected_name):
    vocabulary = Vocabulary(vocab_uri)
    term_class = VocabularyTerm.from_vocab(vocabulary, name=name_param)
    assert term_class.__name__ == expected_name


def test_vocabulary_find(monkeypatch, vocabulary_graph):
    mock_get_vocabulary = MagicMock()
    mock_get_vocabulary.return_value = vocabulary_graph
    monkeypatch.setattr(plastron.validation.vocabularies, 'get_vocabulary_graph', mock_get_vocabulary)

    vocabulary = Vocabulary('http://example.com/vocab#')
    term = vocabulary.find(rdf.value, Literal('foo foo foo'))
    assert term[rdfs.comment] == Literal('test value')


def test_vocabulary_find_error(monkeypatch, vocabulary_graph):
    mock_get_vocabulary = MagicMock()
    mock_get_vocabulary.return_value = vocabulary_graph
    monkeypatch.setattr(plastron.validation.vocabularies, 'get_vocabulary_graph', mock_get_vocabulary)

    vocabulary = Vocabulary('http://example.com/vocab#')
    with pytest.raises(KeyError):
        vocabulary.find(rdf.value, Literal('NO SUCH VALUE'))
