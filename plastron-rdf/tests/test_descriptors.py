import pytest
from rdflib import URIRef

from plastron.rdfmapping.descriptors import Property, ObjectProperty, DataProperty
from plastron.rdfmapping.properties import RDFProperty, RDFObjectProperty, RDFDataProperty


class MockResource:
    pass


class Foo(MockResource):
    title = Property(URIRef('http://purl.org/dc/terms/title'), required=True, repeatable=False)
    subject = ObjectProperty(URIRef('http://purl.org/dc/terms/subject'), required=True, repeatable=False)
    identifier = DataProperty(URIRef('http://purl.org/dc/terms/identifier'), required=True, repeatable=False)


@pytest.mark.parametrize(
    ('prop_name', 'prop_type'),
    [
        ('title', Property),
        ('subject', ObjectProperty),
        ('identifier', DataProperty),
    ]
)
def test_owner_get(prop_name, prop_type):
    prop = getattr(Foo, prop_name)

    assert isinstance(prop, prop_type)
    assert prop.name == prop_name
    assert prop.required
    assert not prop.repeatable


@pytest.mark.parametrize(
    ('prop_name', 'prop_type'),
    [
        ('title', RDFProperty),
        ('subject', RDFObjectProperty),
        ('identifier', RDFDataProperty),
    ]
)
def test_instance_get(prop_name, prop_type):
    instance = Foo()
    prop = getattr(instance, prop_name)

    assert isinstance(prop, prop_type)
    assert prop.attr_name == prop_name
    assert prop.required
    assert not prop.repeatable
