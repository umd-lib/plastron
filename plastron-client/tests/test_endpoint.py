import pytest

from plastron.client import Endpoint


@pytest.fixture
def endpoint():
    return Endpoint(url='http://localhost:8080/fcrepo/rest')


def test_contains(endpoint):
    assert endpoint.contains('http://localhost:8080/fcrepo/rest/123')
    assert 'http://localhost:8080/fcrepo/rest/123' in endpoint


def test_not_contains(endpoint):
    assert not endpoint.contains('http://example.com/123')
    assert 'http://example.com/123' not in endpoint


def test_contains_with_external_url():
    endpoint = Endpoint(
        url='http://localhost:8080/fcrepo/rest',
        external_url='https://repo.example.net',
    )

    assert 'https://repo.example.net/123' in endpoint
    assert 'http://localhost:8080/fcrepo/rest/123' in endpoint
