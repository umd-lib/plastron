import pytest
from rdflib import Literal, URIRef

from plastron.namespaces import rdfs, owl, dcterms
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.embed import EmbeddedObject
from plastron.rdfmapping.resources import RDFResourceBase
from plastron.serializers.csv import join_values, flatten_headers, unflatten, flatten, ensure_text_mode, \
    ensure_binary_mode


@pytest.fixture
def header_map():
    return {
        'title': 'Title',
        'subject': {
            'label': 'Subject',
            'same_as': 'Subject URI',
        },
    }


class Subject(RDFResourceBase):
    label = DataProperty(rdfs.label)
    same_as = ObjectProperty(owl.sameAs)


class Thing(RDFResourceBase):
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
    output_row = {k: join_values(v) for k, v in flatten(obj, header_map).items()}

    for key in row.keys():
        assert output_row[key] == row[key]


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
