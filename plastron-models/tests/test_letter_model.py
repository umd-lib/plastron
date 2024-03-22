from plastron.models.letter import Letter
from plastron.namespaces import ore
from rdflib import Graph, URIRef

base_uri = 'http://example.com/xyz'


def test_letter_without_presentation_set_is_valid():
    letter = Letter()
    # Required fields
    letter.identifier = 'test_letter'
    letter.object_type = 'http://purl.org/dc/dcmitype/Text'
    letter.rights = 'http://vocab.lib.umd.edu/rightsStatement#InC-EDU'
    letter.title = 'Test Letter'
    letter.description = 'Test Letter Description'
    letter.language = 'en'
    letter.part_of = 'http://fedora.info/definitions/v4/repository#inaccessibleResource'
    letter.bibliographic_citation = 'Test Bibliographic Citation'
    letter.rights_holder = 'Test Rights Holder'
    letter.type = 'http://purl.org/dc/dcmitype/Text'
    letter.extent = '1 page'
    assert letter.is_valid


def test_letter_with_single_presentation_set():
    letter = create_letter_with_presentation_set(
        base_uri, ['foo']
    )

    assert len(letter.presentation_set) == 1
    assert letter.presentation_set.value == URIRef('http://vocab.lib.umd.edu/set#foo')


def test_letter_validation_with_valid_presentation_set():
    letter = create_letter_with_presentation_set(
        base_uri, ['test']
    )
    assert letter.presentation_set.is_valid


def test_letter_validation_with_invalid_presentation_set():
    letter = create_letter_with_presentation_set(
        base_uri, ['not_valid']
    )
    assert not letter.presentation_set.is_valid


def test_letter_with_multiple_presentation_sets():
    letter = create_letter_with_presentation_set(
        base_uri, ['foo', 'bar']
    )

    assert len(letter.presentation_set) == 2

    expected = sorted([URIRef('http://vocab.lib.umd.edu/set#bar'), URIRef('http://vocab.lib.umd.edu/set#foo')])
    assert sorted(list(letter.presentation_set.values)) == expected


def test_single_presentation_set_can_be_set_on_letter():
    letter = Letter(uri=URIRef('http://example.com/123'), presentation_set=URIRef('http://vocab.lib.umd.edu/set#foobar'))

    expected = (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar'))
    assert expected in letter.graph


def test_multiple_presentation_sets_can_be_set_on_letter():
    base_uri = URIRef('http://example.com/123')

    graph = Graph()
    graph.add((base_uri, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')))
    graph.add((base_uri, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz')))

    letter = Letter(uri=base_uri, presentation_set=graph.objects())

    expected = [
        (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')),
        (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz'))
    ]

    for e in expected:
        assert e in letter.graph


# Helper Functions

def create_presentation_set_turtle_format(set_names):
    preamble = '@prefix ore: <http://www.openarchives.org/ore/terms/> .'
    preamble = preamble + '@prefix umdset: <http://vocab.lib.umd.edu/set#> .'
    presentation_set = preamble
    for set_name in set_names:
        presentation_set += f'<> ore:isAggregatedBy umdset:{set_name} .'
    return presentation_set


def create_letter_with_presentation_set(item_uri, set_names):
    presentation_set = create_presentation_set_turtle_format(set_names)
    return Letter(
        graph=Graph().parse(data=presentation_set, format='turtle', publicID=base_uri),
        uri=item_uri
    )
