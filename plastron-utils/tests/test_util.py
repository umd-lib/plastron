import pytest

from argparse import ArgumentParser, ArgumentTypeError

from plastron.namespaces import dcterms, rdf, pcdm
from plastron.cli import parse_data_property, parse_object_property
from plastron.utils import uri_or_curie
from rdflib.term import URIRef, Literal

INVALID_URI_OR_CURIE_ARGS = [
    # None
    None,
    # empty string
    '',
    # unrecognized namespace
    'not_in_namespace:Foo'
]
VALID_URI_OR_CURIE_ARGS = [
    # CURIE
    'umdaccess:Public',
    # N3-formatted URI
    '<http://vocab.lib.umd.edu/access#Public>',
    # plain string HTTP URI
    'http://vocab.lib.umd.edu/access#Public'
]
EXPECTED_TERM = URIRef('http://vocab.lib.umd.edu/access#Public')


# Tests for the "uri_or_curie" function

@pytest.mark.parametrize(
    'arg_value', INVALID_URI_OR_CURIE_ARGS
)
def test_given_invalid_uri_or_curie__raises_error(arg_value):
    with pytest.raises(ArgumentTypeError):
        uri_or_curie(arg_value)


@pytest.mark.parametrize(
    'arg_value', VALID_URI_OR_CURIE_ARGS
)
def test_given_valid_uri_or_curie__returns_term(arg_value):
    assert uri_or_curie(arg_value) == EXPECTED_TERM


@pytest.mark.parametrize(
    'arg_value', VALID_URI_OR_CURIE_ARGS
)
def test_given_valid_uri_or_curie_type__parse_args_returns_uriref(arg_value):
    parser = ArgumentParser()
    parser.add_argument('--access', type=uri_or_curie)
    args = parser.parse_args(('--access', arg_value))
    assert isinstance(args.access, URIRef)
    assert args.access == EXPECTED_TERM


@pytest.mark.parametrize(
    'arg_value', INVALID_URI_OR_CURIE_ARGS
)
def test_given_invalid_uri_or_curie_type__parse_args_exits(arg_value):
    parser = ArgumentParser()
    parser.add_argument('--access', type=uri_or_curie)
    with pytest.raises(SystemExit):
        parser.parse_args(('--access', arg_value))


@pytest.mark.parametrize(
    ('p', 'o', 'expected'),
    [
        ('dcterms:title', 'Foobar', (dcterms.title, Literal('Foobar'))),
        ('dcterms:title', '"der Hund"@de', (dcterms.title, Literal('der Hund', lang='de')))
    ]
)
def test_parse_data_property(p, o, expected):
    assert parse_data_property(p, o) == expected


@pytest.mark.parametrize(
    ('p', 'o', 'expected'),
    [
        ('rdf:type', 'pcdm:Object', (rdf.type, pcdm.Object)),
        ('dcterms:creator', 'https://www.lib.umd.edu/', (dcterms.creator, URIRef('https://www.lib.umd.edu/'))),
        ('dcterms:creator', '<https://www.lib.umd.edu/>', (dcterms.creator, URIRef('https://www.lib.umd.edu/')))
    ]
)
def test_parse_object_property(p, o, expected):
    assert parse_object_property(p, o) == expected
