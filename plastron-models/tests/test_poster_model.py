from plastron.models.poster import Poster
from plastron.namespaces import ore
from rdflib import Graph, Literal, URIRef

import pytest

base_uri = 'http://example.com/xyz'


@pytest.mark.skip('Test cannot be run because Poster validation does not currently work')
def test_poster_without_presentation_set_is_valid():
    poster = Poster()
    # Required fields
    poster.identifier = 'test_poster'
    poster.title = 'Test Poster'
    poster.language = 'en'
    poster.part_of = 'http://fedora.info/definitions/v4/repository#inaccessibleResource'
    poster.type = Literal('http://purl.org/dc/dcmitype/Image')
    poster.format = "Test Poster Format"
    poster.locator = 'NZK120'
    poster.rights = 'http://rightsstatements.org/vocab/NoC-US/1.0/'

    assert poster.is_valid


def test_poster_with_single_presentation_set():
    poster = create_poster_with_presentation_set(
        base_uri, ['foo']
    )

    assert len(poster.presentation_set) == 1
    assert poster.presentation_set.value == URIRef('http://vocab.lib.umd.edu/set#foo')


def test_poster_validation_with_valid_presentation_set():
    poster = create_poster_with_presentation_set(
        base_uri, ['test']
    )
    assert poster.presentation_set.is_valid


def test_poster_validation_with_invalid_presentation_set():
    poster = create_poster_with_presentation_set(
        base_uri, ['not_valid']
    )
    assert not poster.presentation_set.is_valid


def test_poster_with_multiple_presentation_sets():
    poster = create_poster_with_presentation_set(
        base_uri, ['foo', 'bar']
    )

    assert len(poster.presentation_set) == 2

    expected = sorted([URIRef('http://vocab.lib.umd.edu/set#bar'), URIRef('http://vocab.lib.umd.edu/set#foo')])
    assert sorted(list(poster.presentation_set.values)) == expected


def test_single_presentation_set_can_be_set_on_poster():
    poster = Poster(uri=URIRef('http://example.com/123'), presentation_set=URIRef('http://vocab.lib.umd.edu/set#foobar'))

    expected = (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar'))
    assert expected in poster.graph


def test_multiple_presentation_sets_can_be_set_on_poster():
    base_uri = URIRef('http://example.com/123')

    graph = Graph()
    graph.add((base_uri, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')))
    graph.add((base_uri, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz')))

    poster = Poster(uri=base_uri, presentation_set=graph.objects())

    expected = [
        (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')),
        (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz'))
    ]

    for e in expected:
        assert e in poster.graph


# Helper Functions

def create_presentation_set_turtle_format(set_names):
    preamble = '@prefix ore: <http://www.openarchives.org/ore/terms/> .'
    preamble = preamble + '@prefix umdset: <http://vocab.lib.umd.edu/set#> .'
    presentation_set = preamble
    for set_name in set_names:
        presentation_set += f'<> ore:isAggregatedBy umdset:{set_name} .'
    return presentation_set


def create_poster_with_presentation_set(item_uri, set_names):
    presentation_set = create_presentation_set_turtle_format(set_names)
    return Poster(
        graph=Graph().parse(data=presentation_set, format='turtle', publicID=base_uri),
        uri=item_uri
    )
