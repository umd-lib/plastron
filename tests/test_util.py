import pytest

from argparse import ArgumentParser, ArgumentTypeError
from plastron.util import uri_or_curie
from rdflib.term import URIRef


# Tests for the "uri_or_curie" function

def test_given_None__raises_ArgumentTypeError():
    with pytest.raises(ArgumentTypeError):
        uri_or_curie(None)


def test_given_empty_string__raises_ArgumentTypeError():
    with pytest.raises(ArgumentTypeError):
        uri_or_curie('')


def test_given_invalid_curie__raises_ArgumentTypeError():
    with pytest.raises(ArgumentTypeError):
        uri_or_curie('not_in_namespace:Foo')


def test_given_valid_curie__returns_term():
    assert uri_or_curie('umdaccess:Public') == URIRef('http://vocab.lib.umd.edu/access#Public')


def test_given_valid_N3_URI__returns_term():
    assert uri_or_curie('<http://vocab.lib.umd.edu/access#Public>') == \
        URIRef('http://vocab.lib.umd.edu/access#Public')


def test_given_valid_simple_URI__returns_term():
    assert uri_or_curie('http://vocab.lib.umd.edu/access#Public') == \
        URIRef('http://vocab.lib.umd.edu/access#Public')


@pytest.mark.parametrize(
    'arg_value', [
        # CURIE
        'umdaccess:Public',
        # N3-formatted URI
        '<http://vocab.lib.umd.edu/access#Public>',
        # plain string HTTP URI
        'http://vocab.lib.umd.edu/access#Public'
    ]
)
def test_given_valid_uri_or_curie_type__parse_args_returns_uriref(arg_value):
    parser = ArgumentParser()
    parser.add_argument('--access', type=uri_or_curie)
    args = parser.parse_args(('--access', arg_value))
    assert isinstance(args.access, URIRef)


@pytest.mark.parametrize(
    'arg_value', [
        None,
        '',
        'not_in_namespace:Foo'
    ]
)
def test_given_invalid_uri_or_curie_type__parse_args_exits(arg_value):
    parser = ArgumentParser()
    parser.add_argument('--access', type=uri_or_curie)
    with pytest.raises(SystemExit):
        parser.parse_args(('--access', arg_value))
