from plastron.rdfmapping.resources import RDFResource
from rdflib import URIRef


def test_add_properties():
    resource = RDFResource()
    assert len(resource.rdf_type) == 0
    resource.add_properties(rdf_type=URIRef('http://purl.org/dc/dcmitype/Text'))
    assert len(resource.rdf_type) == 1
    resource.add_properties(rdf_type=URIRef('http://purl.org/dc/dcmitype/Image'))
    assert len(resource.rdf_type) == 2
    assert set(resource.rdf_type.values) == {
        URIRef('http://purl.org/dc/dcmitype/Text'),
        URIRef('http://purl.org/dc/dcmitype/Image'),
    }


def test_add_properties_list():
    resource = RDFResource()
    assert len(resource.rdf_type) == 0
    resource.add_properties(rdf_type=[
        URIRef('http://purl.org/dc/dcmitype/Text'),
        URIRef('http://purl.org/dc/dcmitype/Image'),
    ])
    assert len(resource.rdf_type) == 2
    assert set(resource.rdf_type.values) == {
        URIRef('http://purl.org/dc/dcmitype/Text'),
        URIRef('http://purl.org/dc/dcmitype/Image'),
    }


def test_set_properties():
    resource = RDFResource()
    assert len(resource.rdf_type) == 0
    resource.set_properties(rdf_type=URIRef('http://purl.org/dc/dcmitype/Text'))
    assert len(resource.rdf_type) == 1
    resource.set_properties(rdf_type=URIRef('http://purl.org/dc/dcmitype/Image'))
    assert len(resource.rdf_type) == 1
    assert set(resource.rdf_type.values) == {URIRef('http://purl.org/dc/dcmitype/Image')}


def test_set_properties_list():
    resource = RDFResource()
    assert len(resource.rdf_type) == 0
    resource.set_properties(rdf_type=[
        URIRef('http://purl.org/dc/dcmitype/Text'),
        URIRef('http://purl.org/dc/dcmitype/Image'),
    ])
    assert len(resource.rdf_type) == 2
    assert set(resource.rdf_type.values) == {
        URIRef('http://purl.org/dc/dcmitype/Text'),
        URIRef('http://purl.org/dc/dcmitype/Image'),
    }
