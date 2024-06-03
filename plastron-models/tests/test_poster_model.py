from rdflib import Literal

from plastron.models.poster import Poster

base_uri = 'http://example.com/xyz'


def test_poster_invalid_with_no_fields():
    poster = Poster()
    assert not poster.is_valid


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
