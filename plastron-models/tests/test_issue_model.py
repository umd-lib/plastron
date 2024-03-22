from plastron.models.newspaper import Issue
from plastron.namespaces import ore
from rdflib import Graph, URIRef

import pytest

base_uri = 'http://example.com/xyz'


@pytest.mark.skip('Test cannot be run because Issue validation does not currently work')
def test_issue_without_presentation_set_is_valid():
    issue = Issue()
    # Required fields
    issue.identifier = 'test_issue'
    issue.title = 'Test Issue'
    issue.date = '1970-01-01'
    issue.volume = '1'
    issue.issue = '1'
    issue.edition = '1'

    assert issue.is_valid


def test_issue_with_single_presentation_set():
    issue = create_issue_with_presentation_set(
        base_uri, ['foo']
    )

    assert len(issue.presentation_set) == 1
    assert issue.presentation_set.value == URIRef('http://vocab.lib.umd.edu/set#foo')


def test_letter_validation_with_valid_presentation_set():
    issue = create_issue_with_presentation_set(
        base_uri, ['test']
    )
    assert issue.presentation_set.is_valid


def test_issue_validation_with_invalid_presentation_set():
    issue = create_issue_with_presentation_set(
        base_uri, ['not_valid']
    )
    assert not issue.presentation_set.is_valid


def test_issue_with_multiple_presentation_sets():
    issue = create_issue_with_presentation_set(
        base_uri, ['foo', 'bar']
    )

    assert len(issue.presentation_set) == 2

    expected = sorted([URIRef('http://vocab.lib.umd.edu/set#bar'), URIRef('http://vocab.lib.umd.edu/set#foo')])
    assert sorted(list(issue.presentation_set.values)) == expected


def test_single_presentation_set_can_be_set_on_letter():
    issue = Issue(uri=URIRef('http://example.com/123'), presentation_set=URIRef('http://vocab.lib.umd.edu/set#foobar'))

    expected = (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar'))
    assert expected in issue.graph


def test_multiple_presentation_sets_can_be_set_on_issue():
    base_uri = URIRef('http://example.com/123')

    graph = Graph()
    graph.add((base_uri, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')))
    graph.add((base_uri, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz')))

    issue = Issue(uri=base_uri, presentation_set=graph.objects())

    expected = [
        (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')),
        (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz'))
    ]

    for e in expected:
        assert e in issue.graph


# Helper Functions

def create_presentation_set_turtle_format(set_names):
    preamble = '@prefix ore: <http://www.openarchives.org/ore/terms/> .'
    preamble = preamble + '@prefix umdset: <http://vocab.lib.umd.edu/set#> .'
    presentation_set = preamble
    for set_name in set_names:
        presentation_set += f'<> ore:isAggregatedBy umdset:{set_name} .'
    return presentation_set


def create_issue_with_presentation_set(item_uri, set_names):
    presentation_set = create_presentation_set_turtle_format(set_names)
    return Issue(
        graph=Graph().parse(data=presentation_set, format='turtle', publicID=base_uri),
        uri=item_uri
    )
