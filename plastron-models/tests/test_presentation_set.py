from plastron.models.umd import Item
from plastron.models.letter import Letter
from plastron.models.newspaper import Issue
from plastron.models.poster import Poster
from plastron.namespaces import ore
from rdflib import Graph, URIRef

import pytest

base_uri = 'http://example.com/xyz'


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_model_with_single_presentation_set(model_class):
    model = create_model_with_presentation_set(
        model_class, base_uri, ['foo']
    )

    assert len(model.presentation_set) == 1
    assert model.presentation_set.value == URIRef('http://vocab.lib.umd.edu/set#foo')


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_model_validation_with_valid_presentation_set(model_class):
    model = create_model_with_presentation_set(
        model_class, base_uri, ['test']
    )
    assert model.presentation_set.is_valid


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_model_validation_with_invalid_presentation_set(model_class):
    model = create_model_with_presentation_set(
        model_class, base_uri, ['not_valid']
    )
    assert not model.presentation_set.is_valid


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_model_with_multiple_presentation_sets(model_class):
    model = create_model_with_presentation_set(
        model_class, base_uri, ['foo', 'bar']
    )

    assert len(model.presentation_set) == 2

    expected = sorted([URIRef('http://vocab.lib.umd.edu/set#bar'), URIRef('http://vocab.lib.umd.edu/set#foo')])
    assert sorted(list(model.presentation_set.values)) == expected


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_single_presentation_set_can_be_set_on_model(model_class):
    model = model_class(uri=base_uri)
    model.presentation_set = URIRef('http://vocab.lib.umd.edu/set#foobar')

    expected = (URIRef(base_uri), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar'))
    assert expected in model.graph


@pytest.mark.parametrize("model_class", [Item, Letter, Poster, Issue])
def test_multiple_presentation_sets_can_be_set_on_model(model_class):
    base_uri_ref = URIRef(base_uri)

    model = model_class(uri=base_uri)
    model.presentation_set.add(URIRef('http://vocab.lib.umd.edu/set#foobar'))
    model.presentation_set.add(URIRef('http://vocab.lib.umd.edu/set#barbaz'))

    expected = [
        (base_uri_ref, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')),
        (base_uri_ref, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz'))
    ]

    for e in expected:
        assert e in model.graph


# Helper Functions

def create_presentation_set_turtle_format(set_names):
    preamble = '@prefix ore: <http://www.openarchives.org/ore/terms/> .'
    preamble = preamble + '@prefix umdset: <http://vocab.lib.umd.edu/set#> .'
    presentation_set = preamble
    for set_name in set_names:
        presentation_set += f'<> ore:isAggregatedBy umdset:{set_name} .'
    return presentation_set


def create_model_with_presentation_set(model_class, item_uri, set_names):
    presentation_set = create_presentation_set_turtle_format(set_names)
    model_graph = Graph().parse(data=presentation_set, format='turtle', publicID=item_uri)
    model = model_class(graph=model_graph, uri=item_uri)

    return model
