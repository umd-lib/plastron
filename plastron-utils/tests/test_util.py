from argparse import ArgumentParser, ArgumentTypeError

import pytest
from rdflib.term import URIRef

from plastron.utils import uri_or_curie

INVALID_URI_OR_CURIE_ARGS = [
    # None
    None,
    # empty string
    '',
    # unrecognized namespace
    'not_in_namespace:Foo',
]
VALID_URI_OR_CURIE_ARGS = [
    # CURIE
    'umdaccess:Public',
    # N3-formatted URI
    '<http://vocab.lib.umd.edu/access#Public>',
    # plain string HTTP URI
    'http://vocab.lib.umd.edu/access#Public',
]
EXPECTED_TERM = URIRef('http://vocab.lib.umd.edu/access#Public')


# Tests for the "uri_or_curie" function


@pytest.mark.parametrize('arg_value', INVALID_URI_OR_CURIE_ARGS)
def test_given_invalid_uri_or_curie__raises_error(arg_value):
    with pytest.raises(ArgumentTypeError):
        uri_or_curie(arg_value)


@pytest.mark.parametrize('arg_value', VALID_URI_OR_CURIE_ARGS)
def test_given_valid_uri_or_curie__returns_term(arg_value):
    assert uri_or_curie(arg_value) == EXPECTED_TERM


@pytest.mark.parametrize('arg_value', VALID_URI_OR_CURIE_ARGS)
def test_given_valid_uri_or_curie_type__parse_args_returns_uriref(arg_value):
    parser = ArgumentParser()
    parser.add_argument('--access', type=uri_or_curie)
    args = parser.parse_args(('--access', arg_value))
    assert isinstance(args.access, URIRef)
    assert args.access == EXPECTED_TERM


@pytest.mark.parametrize('arg_value', INVALID_URI_OR_CURIE_ARGS)
def test_given_invalid_uri_or_curie_type__parse_args_exits(arg_value):
    parser = ArgumentParser()
    parser.add_argument('--access', type=uri_or_curie)
    with pytest.raises(SystemExit):
        parser.parse_args(('--access', arg_value))
