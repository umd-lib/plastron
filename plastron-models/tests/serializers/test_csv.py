import pytest
from rdflib import URIRef, Literal

from plastron.models import ContentModeledResource
from plastron.models.authorities import Place
from plastron.models.umd import Item
from plastron.namespaces import umdaccess, dcterms, dcmitype
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import DataProperty
from plastron.rdfmapping.embed import embedded, EmbeddedObject
from plastron.rdfmapping.resources import RDFResource
from plastron.serializers import CSVSerializer
from plastron.serializers.csv import unflatten


@pytest.mark.parametrize(
    ('resource', 'expected_values'),
    [
        (Item(), {'PUBLISH': 'False', 'HIDDEN': 'False', 'Presentation Set': ''}),
        (Item(rdf_type=umdaccess.Published), {'PUBLISH': 'True', 'HIDDEN': 'False', 'Presentation Set': ''}),
        (Item(rdf_type=umdaccess.Hidden), {'PUBLISH': 'False', 'HIDDEN': 'True', 'Presentation Set': ''}),
        (
            Item(rdf_type=[umdaccess.Published, umdaccess.Hidden]),
            {'PUBLISH': 'True', 'HIDDEN': 'True', 'Presentation Set': ''},
        ),
        (
            Item(presentation_set=URIRef('http://vocab.lib.umd.edu/set#test')),
            {'PUBLISH': 'False', 'HIDDEN': 'False', 'Presentation Set': 'http://vocab.lib.umd.edu/set#test'},
        ),
        (
            Item(rdf_type=umdaccess.Published, presentation_set=URIRef('http://vocab.lib.umd.edu/set#test')),
            {'PUBLISH': 'True', 'HIDDEN': 'False', 'Presentation Set': 'http://vocab.lib.umd.edu/set#test'},
        ),
        (
            Item(
                rdf_type=[umdaccess.Published, umdaccess.Hidden],
                presentation_set=URIRef('http://vocab.lib.umd.edu/set#test'),
            ),
            {'PUBLISH': 'True', 'HIDDEN': 'True', 'Presentation Set': 'http://vocab.lib.umd.edu/set#test'},
        ),
    ]
)
def test_write(resource, expected_values):
    serializer = CSVSerializer()
    row = serializer.write(resource=resource)
    for key, expected_value in expected_values.items():
        assert row[key] == expected_value


def test_serialize_multiple_languages(multilingual_item, header_map):
    serializer = CSVSerializer()
    row = serializer.write(multilingual_item)
    expected_values = {'Title': 'The Trial|[@de]Der Prozeß'}
    for key, value in expected_values.items():
        assert row[key] == value


@rdf_type(dcmitype.Text)
class SimpleModel(RDFResource, ContentModeledResource):
    title = DataProperty(dcterms.title)
    description = DataProperty(dcterms.description)

    HEADER_MAP = {
        'title': 'Title',
        'description': 'Description',
    }


def test_serialize_all_columns():
    serializer = CSVSerializer()
    obj = SimpleModel()
    row = serializer.write(obj)
    assert len(row) == len(SimpleModel.HEADER_MAP) + len(CSVSerializer.SYSTEM_HEADERS)


def test_round_trip_serialization():
    item = Item()
    item.location.add(embedded(Place)(label=Literal('Baltimore')))
    item.location.add(embedded(Place)(label=Literal('Washington')))
    assert len(item.location) == 2

    serializer = CSVSerializer()
    row = serializer.write(item)
    assert row['Location'] == 'Baltimore;Washington'

    new_item_params = unflatten(row, Item, Item.HEADER_MAP)
    locations = new_item_params['location']
    assert len(locations) == 2
    assert isinstance(locations[0], EmbeddedObject)
    assert locations[0].cls is Place
    assert locations[0].kwargs == {'label': [Literal('Baltimore')]}
    assert isinstance(locations[1], EmbeddedObject)
    assert locations[1].cls is Place
    assert locations[1].kwargs == {'label': [Literal('Washington')]}


def test_multilanguage_round_trip_one_object():
    item = Item()
    item.location.add(embedded(Place)(label=[Literal('Germany'), Literal('Deutschland', lang='de')]))
    assert len(item.location) == 1
    serializer = CSVSerializer()
    row = serializer.write(item)
    assert row['Location'] == 'Germany|[@de]Deutschland'
    new_item_params = unflatten(row, Item, Item.HEADER_MAP)
    locations = new_item_params['location']
    assert len(locations) == 1
    assert isinstance(locations[0], EmbeddedObject)
    assert locations[0].cls is Place
    assert locations[0].kwargs == {'label': [Literal('Germany'), Literal('Deutschland', lang='de')]}


def test_multilanguage_round_trip_two_objects():
    item = Item()
    item.location.add(embedded(Place)(label=Literal('France')))
    item.location.add(embedded(Place)(label=Literal('Deutschland', lang='de')))
    assert len(item.location) == 2
    serializer = CSVSerializer()
    row = serializer.write(item)
    assert row['Location'] == 'France;[@de]Deutschland'
    new_item_params = unflatten(row, Item, Item.HEADER_MAP)
    locations = new_item_params['location']
    assert len(locations) == 2
    assert isinstance(locations[0], EmbeddedObject)
    assert locations[0].cls is Place
    assert locations[0].kwargs == {'label': [Literal('France')]}
    assert isinstance(locations[1], EmbeddedObject)
    assert locations[1].cls is Place
    assert locations[1].kwargs == {'label': [Literal('Deutschland', lang='de')]}
