from rdflib import Graph, Literal, URIRef

from plastron.models.authorities import Agent
from plastron.models.umd import Item
from plastron.namespaces import dcterms, umdtype, rdfs
from plastron.rdfmapping.embed import embedded


def test_identifier_distinct_from_accession_number():
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
    assert len(item.identifier) == 1
    assert item.identifier.value == Literal('foo')

    assert len(item.accession_number) == 1
    assert item.accession_number.value == Literal('1212XN1', datatype=umdtype.accessionNumber)

    assert (URIRef(base_uri), dcterms.identifier, Literal('foo')) in item.graph
    assert (URIRef(base_uri), dcterms.identifier, Literal('1212XN1', datatype=umdtype.accessionNumber)) in item.graph


def test_item_valid_with_only_required_fields():
    item = Item()

    # Only provide required fields
    item.identifier = 'test_item'
    item.object_type = 'http://purl.org/dc/dcmitype/Text'
    item.rights = 'http://vocab.lib.umd.edu/rightsStatement#InC-EDU'
    item.title = 'Test Item'
    assert item.is_valid


def test_audience_property():
    item = Item(uri=URIRef('http://example.com/foo'), audience=embedded(Agent)(label=Literal('John Doe')))

    audience_triples = list(item.graph.triples((URIRef('http://example.com/foo'), dcterms.audience, None)))
    assert len(audience_triples) == 1
    embedded_subject = audience_triples[0][2]
    label_triples = list(item.graph.triples((embedded_subject, rdfs.label, Literal('John Doe'))))
    assert len(label_triples) == 1
