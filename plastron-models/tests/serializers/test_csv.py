import pytest
from rdflib import URIRef

from plastron.models.umd import Item
from plastron.namespaces import umdaccess
from plastron.serializers import CSVSerializer


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
