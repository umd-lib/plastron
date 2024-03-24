from plastron.models.poster import Poster
from plastron.namespaces import ore
from rdflib import Graph, Literal, URIRef

import pytest

base_uri = 'http://example.com/xyz'


@pytest.mark.skip('Test cannot be run because Poster validation does not currently work')
def test_poster_valid_with_only_required_fields():
    poster = Poster()

    # Only provide required fields
    poster.identifier = 'test_poster'
    poster.title = 'Test Poster'
    poster.language = 'en'
    poster.part_of = 'http://fedora.info/definitions/v4/repository#inaccessibleResource'
    poster.type = Literal('http://purl.org/dc/dcmitype/Image')
    poster.format = "Test Poster Format"
    poster.locator = 'NZK120'
    poster.rights = 'http://rightsstatements.org/vocab/NoC-US/1.0/'

    assert poster.is_valid
