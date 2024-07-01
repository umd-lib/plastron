import pytest as pytest
from rdflib import Literal, URIRef

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


@pytest.mark.parametrize(
    ('subject_value', 'expected_validity'),
    [
        ([], True),
        ([Literal('single')], True),
        ([Literal('one'), Literal('two')], True),
    ]
)
def test_poster_subject_field_literals(subject_value, expected_validity):
    poster = Poster(subject=subject_value)
    assert bool(poster.subject.is_valid) == expected_validity


@pytest.mark.parametrize(
    'value',
    [
        ['foo'],
        [12345],
        [URIRef('http://example.com/foo')],
    ]
)
def test_poster_subject_field_non_literal_fails(value):
    with pytest.raises(TypeError):
        Poster(subject=value)
