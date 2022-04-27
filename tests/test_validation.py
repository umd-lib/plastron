from unittest.mock import Mock, patch
from urllib.error import HTTPError

import httpretty
import pytest
from httpretty import GET
from rdflib import URIRef

from plastron.rdf import RDFObjectProperty
from plastron.validation import ValidationError, is_edtf_formatted
from plastron.validation.rules import from_vocabulary, required
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
        # only need one non-empty string to pass
        (['foo', ''], True)
    ]
)
def test_required(values, expected):
    assert required(values) is expected


def test_not_required():
    # no values but not actually required passes
    assert required([], False) is True


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
    ])
def test_is_edtf_formatted(datetime_string):
    assert is_edtf_formatted(datetime_string) is True


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
    assert from_vocabulary(prop, vocab_uri) is expected


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
    mock_graph.parse.side_effect = HTTPError('http://example.org/foo/', 503, '', {}, None)
    MockGraph.return_value = mock_graph
    # failure to retrieve the vocabulary over HTTP should
    # raise a ValidationError
    pytest.raises(ValidationError, get_vocabulary, 'http://example.org/foo/')


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
    prop = RDFObjectProperty()
    prop.values = [URIRef('http://vocab.lib.umd.edu/form#slides_photographs')]
    assert from_vocabulary(prop, 'http://vocab.lib.umd.edu/form')
