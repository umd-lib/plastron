import pytest
from plastron.models.umd import Item
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.embed import EmbeddedObject
from plastron.rdfmapping.resources import RDFResource
from plastron.serializers.csv import (
    ColumnHeader,
    ensure_binary_mode,
    ensure_text_mode,
    flatten,
    flatten_basic_property,
    flatten_headers,
    join_values,
    not_empty,
    unflatten,
)
from rdflib import Literal, URIRef

from plastron.namespaces import dcterms, owl, rdfs, umdtype


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        (None, False),
        ('', False),
        (' ', True),
        ('foo', True),
    ]
)
def test_not_empty(value, expected):
    assert not_empty(value) == expected


class Subject(RDFResource):
    label = DataProperty(rdfs.label)
    same_as = ObjectProperty(owl.sameAs)


class Thing(RDFResource):
    title = DataProperty(dcterms.title)
    subject = ObjectProperty(dcterms.subject, cls=Subject, repeatable=True)


@pytest.mark.parametrize(
    ('values', 'expected'),
    [
        (None, ''),
        ([], ''),
        ([''], ''),
        (['', ''], '|'),
        ([['', ''], ['', '']], '|;|'),
        (['', 'a'], '|a'),
        (['b', ''], 'b|'),
        (['ab', 'cd'], 'ab|cd'),
        ([['w', 'x'], ['y', 'z']], 'w|x;y|z'),
        ([['x'], 'y', ['a', 'b']], 'x;y;a|b'),
    ]
)
def test_join_values(values, expected):
    assert join_values(values) == expected


def test_flatten_headers(header_map):
    flat_headers = {
        'Title': 'title',
        'Subject': 'subject.label',
        'Subject URI': 'subject.same_as',
    }
    assert flatten_headers(header_map) == flat_headers


def test_flatten_multiple_languages(multilingual_item, header_map):
    expected_values = {
        ColumnHeader(label='Title'): ['The Trial', '[@de]Der Prozeß'],
    }
    columns = flatten(multilingual_item, header_map)
    for key, value in expected_values.items():
        assert columns[key] == value

def test_flatten_handle_property():
    """Test that handle properties serialize correctly."""
    item = Item(handle=Literal('hdl:1903.1/1234', datatype=umdtype.handle))
    result = flatten_basic_property(item, 'handle')
    assert result == ['hdl:1903.1/1234']

def test_flatten_accession_number_property():
    """Test that accession_number properties serialize correctly."""
    item = Item(accession_number=Literal('2008-51', datatype=umdtype.accessionNumber))
    result = flatten_basic_property(item, 'accession_number')
    assert result == ['2008-51']


def test_unflatten(header_map):
    row = {
        'Title': 'Foo',
        'Subject': 'Philosophy',
        'Subject URI': 'http://example.com/term/phil',
    }
    params = unflatten(row_data=row, header_map=header_map, resource_class=Thing)
    assert params['title'] == [Literal('Foo')]
    obj = params['subject'][0]
    assert isinstance(obj, EmbeddedObject)
    assert obj.cls is Subject
    assert obj.kwargs == {
        'label': [Literal('Philosophy')],
        'same_as': [URIRef('http://example.com/term/phil')],
    }


def test_unflatten_flatten(header_map):
    row = {
        'Title': 'Foo',
        'Subject': 'Philosophy',
        'Subject URI': 'http://example.com/term/phil',
    }
    obj = Thing(**unflatten(row_data=row, header_map=header_map, resource_class=Thing))
    columns = flatten(obj, header_map)
    output_row = {k: join_values(v) for k, v in columns.items()}

    for key in row.keys():
        assert output_row[ColumnHeader(label=key)] == row[key]


def test_unflatten_multiple_embed(header_map):
    row = {
        'Title': 'Foo',
        'Subject': 'Linguistics;Philosophy',
        'Subject URI': 'http://example.com/term/ling;http://example.com/term/phil',
    }
    params = unflatten(row_data=row, header_map=header_map, resource_class=Thing)
    assert params['title'] == [Literal('Foo')]
    obj_0 = params['subject'][0]
    assert isinstance(obj_0, EmbeddedObject)
    assert obj_0.cls is Subject
    assert obj_0.kwargs == {
        'label': [Literal('Linguistics')],
        'same_as': [URIRef('http://example.com/term/ling')],
    }
    obj_1 = params['subject'][1]
    assert isinstance(obj_1, EmbeddedObject)
    assert obj_1.cls is Subject
    assert obj_1.kwargs == {
        'label': [Literal('Philosophy')],
        'same_as': [URIRef('http://example.com/term/phil')],
    }


def test_ensure_text_mode(datadir):
    with ensure_text_mode((datadir / 'data.csv').open(mode='rb')) as file:
        assert 'b' not in file.mode
    with ensure_text_mode((datadir / 'data.csv').open(mode='r')) as file:
        assert 'b' not in file.mode


def test_ensure_binary_mode(datadir):
    with ensure_binary_mode((datadir / 'data.csv').open(mode='r')) as file:
        assert 'b' in file.mode
    with ensure_binary_mode((datadir / 'data.csv').open(mode='rb')) as file:
        assert 'b' in file.mode
