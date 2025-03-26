import pytest
from rdflib import URIRef, Literal

from plastron.namespaces import xsd
from plastron.rdfmapping.descriptors import DataProperty
from plastron.serializers.csv import ColumnHeader, get_literal


@pytest.mark.parametrize(
    ('descriptor', 'column_header', 'value', 'expected_literal'),
    [
        (
            DataProperty(predicate=URIRef('http://example.com/test'), datatype=None),
            ColumnHeader('Test'),
            'foobar',
            Literal('foobar', datatype=None, lang=None),
        ),
        (
            DataProperty(predicate=URIRef('http://example.com/test'), datatype=xsd.integer),
            ColumnHeader('Test'),
            '123',
            Literal(123, datatype=xsd.integer, lang=None),
        ),
        (
            DataProperty(predicate=URIRef('http://example.com/test'), datatype=None),
            ColumnHeader('Test', language='de'),
            'der Hund',
            Literal('der Hund', datatype=None, lang='de'),
        ),
        (
            DataProperty(predicate=URIRef('http://example.com/test'), datatype=None),
            ColumnHeader('Test'),
            '[@de]der Hund',
            Literal('der Hund', datatype=None, lang='de'),
        ),
    ]
)
def test_get_literal(descriptor, column_header, value, expected_literal):
    assert get_literal(column_header, descriptor, value) == expected_literal


def test_cannot_mix_language_and_datatype():
    with pytest.raises(RuntimeError):
        get_literal(
            ColumnHeader('Test', language='de'),
            DataProperty(predicate=URIRef('http://example.com/test'), datatype=xsd.integer),
            'der Hund',
        )
