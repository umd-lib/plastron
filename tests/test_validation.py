import pytest
from plastron.rdf import RDFObjectProperty
from plastron.validation import is_edtf_formatted
from plastron.validation.rules import from_vocabulary, required
from rdflib import URIRef


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
