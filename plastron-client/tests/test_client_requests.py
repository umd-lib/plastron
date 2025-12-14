from unittest.mock import MagicMock

import pytest
from requests import Session

from plastron.client import Client, Endpoint


class MockOKResponse:
    ok = True
    status_code = 200
    reason = 'OK'


@pytest.mark.parametrize(
    ('method', 'expected_http_method'),
    [
        ('get', 'GET'),
        ('post', 'POST'),
        ('put', 'PUT'),
        ('patch', 'PATCH'),
        ('delete', 'DELETE'),
    ]
)
def test_client_requests(endpoint, method, expected_http_method):
    session = MagicMock(spec=Session)
    session.request.return_value = MockOKResponse()
    client = Client(endpoint=endpoint, session=session)
    url = 'http://localhost:8080/fcrepo/rest/foo/123'
    response = getattr(client, method)(url)
    session.request.assert_called_with(expected_http_method, url)
    assert response.status_code == 200
    assert response.reason == 'OK'
