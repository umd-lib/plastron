import pytest

from argparse import ArgumentTypeError
from plastron.util import uri_or_curie
from rdflib.term import URIRef


class TestUriOrCurie:
    # Tests for the "uri_or_curie" function

    def test_given_None__raises_ArgumentTypeError(self):
        with pytest.raises(ArgumentTypeError):
            uri_or_curie(None)

    def test_given_empty_string__raises_ArgumentTypeError(self):
        with pytest.raises(ArgumentTypeError):
            uri_or_curie('')

    def test_given_invalid_curie__raises_ArgumentTypeError(self):
        with pytest.raises(ArgumentTypeError):
            uri_or_curie('not_in_namespace:Foo')

    def test_given_valid_curie__returns_term(self):
        assert uri_or_curie('umdaccess:Public') == URIRef('http://vocab.lib.umd.edu/access#Public')

    def test_given_valid_N3_URI__returns_term(self):
        assert uri_or_curie('<http://vocab.lib.umd.edu/access#Public>') == \
            URIRef('http://vocab.lib.umd.edu/access#Public')

    def test_given_valid_simple_URI__returns_term(self):
        assert uri_or_curie('http://vocab.lib.umd.edu/access#Public') == \
            URIRef('http://vocab.lib.umd.edu/access#Public')
