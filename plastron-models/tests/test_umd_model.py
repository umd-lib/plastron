from rdflib import Graph, Literal, Namespace, URIRef

from plastron.models.umd import Item
from plastron.namespaces import umdform
from plastron.repo.pcdm import get_new_member_title

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
