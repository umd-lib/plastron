from plastron.models.umd import Item
from plastron.models.letter import Letter
from plastron.models.newspaper import Issue
from plastron.models.poster import Poster
from plastron.namespaces import dcterms
from rdflib import Graph, URIRef

import pytest

base_uri = 'http://example.com/xyz'


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_model_with_terms_of_use(model_class):
    model = create_model_with_terms_of_use(
        model_class, base_uri, 'foo'
    )

    assert model.terms_of_use.value == URIRef('http://vocab.lib.umd.edu/termsOfUse#foo')


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_model_validation_with_valid_terms_of_use(model_class):
    model = create_model_with_terms_of_use(
        model_class, base_uri, 'test'
    )
    assert model.terms_of_use.is_valid


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_model_validation_with_invalid_terms_of_use(model_class):
    model = create_model_with_terms_of_use(
        model_class, base_uri, 'not_valid'
    )
    assert not model.terms_of_use.is_valid


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_terms_of_use_can_be_set_on_model(model_class):
    model = model_class(uri=base_uri)
    model.terms_of_use = URIRef('http://vocab.lib.umd.edu/termsOfUse#test')

    expected = (URIRef(base_uri), dcterms.license, URIRef('http://vocab.lib.umd.edu/termsOfUse#test'))
    assert expected in model.graph
    assert model.terms_of_use.is_valid

# Helper Functions


def create_terms_of_use_turtle_format(term_of_use):
    preamble = '@prefix dcterms: <http://purl.org/dc/terms/> .'
    preamble = preamble + '@prefix umdtermsOfUse: <http://vocab.lib.umd.edu/termsOfUse#> .'
    terms_of_use = preamble
    terms_of_use += f'<> dcterms:license umdtermsOfUse:{term_of_use} .'
    return terms_of_use


def create_model_with_terms_of_use(model_class, item_uri, terms_of_use):
    terms_of_use = create_terms_of_use_turtle_format(terms_of_use)

    model_graph = Graph().parse(data=terms_of_use, format='turtle', publicID=item_uri)
    model = model_class(graph=model_graph, uri=item_uri)

    return model
