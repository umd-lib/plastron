from plastron.models.umd import Item
from rdflib import Graph, Literal, URIRef

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

def test_identifier_distinct_from_accession_number():
    assert len(item.identifier) == 1
    assert item.identifier[0] == Literal('foo')

    assert len(item.accession_number) == 1
    assert item.accession_number[0] == Literal('1212XN1', datatype=URIRef('http://vocab.lib.umd.edu/datatype#accessionNumber'))

    graph = item.graph()
    assert (URIRef(base_uri), URIRef('http://purl.org/dc/terms/identifier'), Literal('foo')) in graph

