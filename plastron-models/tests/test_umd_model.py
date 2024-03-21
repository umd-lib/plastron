from plastron.models.umd import Item
from plastron.namespaces import umdform
from plastron.repo.pcdm import get_new_member_title
from rdflib import Graph, Literal, Namespace, URIRef

rdf = (
    '@prefix dcterms: <http://purl.org/dc/terms/> .'
    '@prefix umdtype: <http://vocab.lib.umd.edu/datatype#> .'
    '<> dcterms:identifier "foo" .'
    '<> dcterms:identifier "1212XN1"^^umdtype:accessionNumber .'
)

base_uri = 'http://example.com/xyz'
item = Item(
    graph=Graph().parse(data=rdf, format='turtle', publicID=base_uri),
    uri=base_uri
)
dcterms = Namespace('http://purl.org/dc/terms/')
umdtype = Namespace('http://vocab.lib.umd.edu/datatype#')
ore = Namespace('http://www.openarchives.org/ore/terms/')


def test_identifier_distinct_from_accession_number():
    assert len(item.identifier) == 1
    assert item.identifier.value == Literal('foo')

    assert len(item.accession_number) == 1
    assert item.accession_number.value == Literal('1212XN1', datatype=umdtype.accessionNumber)

    assert (URIRef(base_uri), dcterms.identifier, Literal('foo')) in item.graph
    assert (URIRef(base_uri), dcterms.identifier, Literal('1212XN1', datatype=umdtype.accessionNumber)) in item.graph


def test_get_new_member():
    basic_item = Item()
    title = get_new_member_title(basic_item, 'foo', 1)
    assert str(title) == 'Page 1'


def test_get_new_member_pool_report():
    pool_report = Item(format=umdform.pool_reports)

    body_title = get_new_member_title(pool_report, 'body-processed-redacted', 1)
    assert str(body_title) == 'Body'

    attachment_title = get_new_member_title(pool_report, 'foo', 2)
    assert str(attachment_title) == 'Attachment 1'


def test_item_without_presentation_set_is_valid():
    item = Item()
    # Required fields
    item.identifier = 'test_item'
    item.object_type = 'http://purl.org/dc/dcmitype/Text'
    item.rights = 'http://vocab.lib.umd.edu/rightsStatement#InC-EDU'
    item.title = 'Test Item'
    assert item.is_valid


def test_item_with_single_presentation_set():
    item = create_item_with_presentation_set(
        base_uri, ['foo']
    )

    assert len(item.presentation_set) == 1
    assert item.presentation_set.value == URIRef('http://vocab.lib.umd.edu/set#foo')


def test_item_validation_with_valid_presentation_set():
    item = create_item_with_presentation_set(
        base_uri, ['test']
    )
    assert item.presentation_set.is_valid


def test_item_validation_with_invalid_presentation_set():
    item = create_item_with_presentation_set(
        base_uri, ['not_valid']
    )
    assert not item.presentation_set.is_valid


def test_item_with_multiple_presentation_sets():
    item = create_item_with_presentation_set(
        base_uri, ['foo', 'bar']
    )

    assert len(item.presentation_set) == 2

    expected = sorted([URIRef('http://vocab.lib.umd.edu/set#bar'), URIRef('http://vocab.lib.umd.edu/set#foo')])
    assert sorted(list(item.presentation_set.values)) == expected


def test_single_presentation_set_can_be_set_on_item():
    item = Item(uri=URIRef('http://example.com/123'), presentation_set=URIRef('http://vocab.lib.umd.edu/set#foobar'))

    expected = (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar'))
    assert expected in item.graph


def test_multiple_presentation_sets_can_be_set_on_item():
    base_uri = URIRef('http://example.com/123')

    graph = Graph()
    graph.add((base_uri, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')))
    graph.add((base_uri, ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz')))

    item = Item(uri=base_uri, presentation_set=graph.objects())

    expected = [
        (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#foobar')),
        (URIRef('http://example.com/123'), ore.isAggregatedBy, URIRef('http://vocab.lib.umd.edu/set#barbaz'))
    ]

    for e in expected:
        assert e in item.graph


# Helper Functions

def create_presentation_set_turtle_format(set_names):
    preamble = '@prefix ore: <http://www.openarchives.org/ore/terms/> .'
    preamble = preamble + '@prefix umdset: <http://vocab.lib.umd.edu/set#> .'
    presentation_set = preamble
    for set_name in set_names:
        presentation_set += f'<> ore:isAggregatedBy umdset:{set_name} .'
    return presentation_set


def create_item_with_presentation_set(item_uri, set_names):
    presentation_set = create_presentation_set_turtle_format(set_names)
    return Item(
        graph=Graph().parse(data=presentation_set, format='turtle', publicID=base_uri),
        uri=item_uri
    )
