from plastron.models.umd import Item, umdform
from rdflib import Graph, Literal, Namespace, URIRef

from plastron.pcdm import Page

rdf = (
    '@prefix dcterms: <http://purl.org/dc/terms/> .'
    '@prefix umdtype: <http://vocab.lib.umd.edu/datatype#> .'
    '<> dcterms:identifier "foo" .'
    '<> dcterms:identifier "1212XN1"^^umdtype:accessionNumber .'
)

base_uri = 'http://example.com/xyz'
item = Item.from_graph(
    graph=Graph().parse(data=rdf, format='turtle', publicID=base_uri),
    subject=base_uri
)
dcterms = Namespace('http://purl.org/dc/terms/')
umdtype = Namespace('http://vocab.lib.umd.edu/datatype#')


def test_identifier_distinct_from_accession_number():
    assert len(item.identifier) == 1
    assert item.identifier[0] == Literal('foo')

    assert len(item.accession_number) == 1
    assert item.accession_number[0] == Literal('1212XN1', datatype=umdtype.accessionNumber)

    graph = item.graph()
    assert (URIRef(base_uri), dcterms.identifier, Literal('foo')) in graph
    assert (URIRef(base_uri), dcterms.identifier, Literal('1212XN1', datatype=umdtype.accessionNumber)) in graph


def test_get_new_member():
    basic_item = Item()
    page = basic_item.get_new_member('foo', 1)
    assert isinstance(page, Page)
    assert str(page.number) == '1'
    assert str(page.title) == 'Page 1'


def test_get_new_member_pool_report():
    pool_report = Item(object_type=umdform.pool_reports)

    body = pool_report.get_new_member('body-processed-redacted', 1)
    assert isinstance(body, Page)
    assert str(body.number) == '1'
    assert str(body.title) == 'Body'

    attachment = pool_report.get_new_member('foo', 2)
    assert isinstance(attachment, Page)
    assert str(attachment.number) == '2'
    assert str(attachment.title) == 'Attachment 1'
