import pytest

from plastron.client import Endpoint


@pytest.fixture()
def endpoint():
    return Endpoint(url='http://example.com/repo')
