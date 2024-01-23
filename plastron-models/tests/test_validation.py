from email.message import Message
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import httpretty
import pytest
from httpretty import GET
from rdflib import URIRef, Literal

from plastron.namespaces import rdfs
from plastron.rdf.rdf import RDFObjectProperty
from plastron.rdfmapping.descriptors import DataProperty
from plastron.rdfmapping.resources import RDFResourceBase
from plastron.validation import ValidationError
from plastron.validation.rules import is_edtf_formatted, is_handle, is_from_vocabulary, is_valid_iso639_code, \
    is_iso_8601_date
from plastron.validation.vocabularies import get_vocabulary


@pytest.mark.parametrize(
    ('values', 'expected'),
    [
        # no values fails
        ([], False),
        # empty string fails
        ([''], False),
        # blank string fails
        (['  '], False),
        # non-empty strings pass
        (['foo'], True),
        (['0'], True),
        # non-string values pass
        ([0], True),
        ([1.0], True),
        # any empty string fails
        (['foo', ''], False),
    ]
)
def test_required(values, expected):
    class SimpleResource(RDFResourceBase):
        label = DataProperty(rdfs.label, required=True)

    obj = SimpleResource(label=[Literal(str(v)) for v in values])
    assert obj.is_valid == expected


@pytest.mark.parametrize(
    'datetime_string', [
        # dates at 11pm fail in edtf 4.0.1
        # these pass when using edtf-validate 1.1.0
        '2020-07-10T23:44:38Z',
        '2020-07-10T23:15:47Z',
        '2020-07-20T23:52:29Z',
        '2020-07-24T23:46:17Z',
        # same dates, but at 10pm, pass
        '2020-07-10T22:44:38Z',
        '2020-07-10T22:15:47Z',
        '2020-07-20T22:52:29Z',
        '2020-07-24T22:46:17Z',
        # the empty string should validate
        '',
    ])
def test_is_edtf_formatted(datetime_string):
    assert is_edtf_formatted(datetime_string)


@pytest.mark.parametrize(
    'code', [
        'en',
        'eng',
    ]
)
def test_is_valid_iso639_code(code):
    assert is_valid_iso639_code(code)


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        ('2024-01-23', True),
        ('2024', False),
        ('2024-01', False),
        ('01-23', False),
        ('2024/01/23', False),
    ]
)
def test_is_iso_8601_date(value, expected):
    assert is_iso_8601_date(value) == expected


@pytest.mark.parametrize(
    'handle', [
        'hdl:1903.1/foobar',
        'hdl:1903.1/327',
        'hdl:1903.1/asdf',
        'hdl:1234.5/example'
    ]
)
def test_is_handle(handle):
    assert is_handle(handle)


@pytest.mark.parametrize(
    'handle', [
        '',
        '     ',
        '1903.1/foobar',
        'not_a_handle',
        'HDL:1903.1/foobar'
    ]
)
def test_not_handle(handle):
    assert not is_handle(handle)


@pytest.mark.parametrize(
    ('value', 'vocab_uri', 'expected'),
    [
        ('http://purl.org/dc/dcmitype/Image', 'http://purl.org/dc/dcmitype/', True),
        ('http://purl.org/dc/dcmitype/Text', 'http://purl.org/dc/dcmitype/', True),
        ('http://example.com/Text', 'http://purl.org/dc/dcmitype/', False),
    ]
)
def test_from_vocabulary(value, vocab_uri, expected):
    prop = RDFObjectProperty()
    prop.values = [URIRef(value)]
    fn = is_from_vocabulary(vocab_uri)
    assert fn(URIRef(value)) == expected


@patch('plastron.validation.vocabularies.Graph')
def test_vocabulary_file_not_found(MockGraph):
    mock_graph = Mock()
    mock_graph.parse.side_effect = [FileNotFoundError, None]
    MockGraph.return_value = mock_graph
    vocab_graph = get_vocabulary('http://purl.org/dc/dcmitype/')
    # parse should be called twice, once with the file location,
    # and once with the remote URI
    assert mock_graph.parse.call_count == 2
    assert vocab_graph == mock_graph


@patch('plastron.validation.vocabularies.Graph')
def test_remote_vocab_error(MockGraph):
    mock_graph = Mock()
    mock_graph.parse.side_effect = HTTPError('http://example.org/foo/', 503, '', Message(), None)
    MockGraph.return_value = mock_graph
    # failure to retrieve the vocabulary over HTTP should
    # raise a ValidationError
    with pytest.raises(ValidationError):
        get_vocabulary('http://example.org/foo/')


@httpretty.activate
def test_remote_vocab_308_redirect(shared_datadir):
    # simulate a 308 -> 303 -> 200 redirection chain to emulate nginx
    httpretty.register_uri(
        uri='http://vocab.lib.umd.edu/form',
        method=GET,
        status=308,
        adding_headers={
            'Location': 'https://vocab.lib.umd.edu/form'
        }
    )
    httpretty.register_uri(
        uri='https://vocab.lib.umd.edu/form',
        method=GET,
        status=303,
        adding_headers={
            'Location': 'https://vocab.lib.umd.edu/form.json'
        }
    )
    httpretty.register_uri(
        uri='https://vocab.lib.umd.edu/form.json',
        method=GET,
        status=200,
        body=(shared_datadir / 'form.json').read_text(),
        content_type='application/ld+json'
    )
    fn = is_from_vocabulary('http://vocab.lib.umd.edu/form')
    assert fn(URIRef('http://vocab.lib.umd.edu/form#slides_photographs'))
