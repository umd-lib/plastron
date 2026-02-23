from unittest.mock import MagicMock

import pytest
from requests import Session

from plastron.client import Endpoint
from plastron.client.proxied import ProxiedClient


@pytest.mark.parametrize(
    ('endpoint_url', 'origin_endpoint_url', 'expected_proto', 'expected_host'),
    [
        (
            'https://example.com/fcrepo/rest',
            'http://localhost:8080/fcrepo/rest',
            'https',
            'example.com'
        ),
        (
            'http://example.net:8080/fcrepo/rest',
            'http://localhost:8080/fcrepo/rest',
            'http',
            'example.net:8080'
        ),
    ]
)
def test_proxied_client_headers(endpoint_url, origin_endpoint_url, expected_proto, expected_host):
    client = ProxiedClient(
        endpoint=Endpoint(endpoint_url),
        origin_endpoint=Endpoint(origin_endpoint_url),
    )
    assert client.session.headers['X-Forwarded-Proto'] == expected_proto
    assert client.session.headers['X-Forwarded-Host'] == expected_host


def test_proxied_client_request():
    mock_session = MagicMock(spec=Session, headers={})
    client = ProxiedClient(
        endpoint=Endpoint('https://example.com/fcrepo/rest'),
        origin_endpoint=Endpoint('http://localhost:8080/fcrepo/rest'),
        session=mock_session,
    )
    client.get('https://example.com/fcrepo/rest/dc/2021')
    mock_session.request.assert_called_once_with('GET', 'http://localhost:8080/fcrepo/rest/dc/2021')
