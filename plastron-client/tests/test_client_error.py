from unittest.mock import MagicMock

from requests import Response

from plastron.client import ClientError


def test_client_error_str():
    response = MagicMock(spec=Response, status_code=404, reason='Not Found')
    error = ClientError(response)
    assert str(error) == '404 Not Found'
