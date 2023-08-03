import pytest

from plastron.client import Endpoint, Client
from plastron.client.auth import get_authenticator


@pytest.fixture
def repo_base_config():
    """Required parameters for Repository configuration"""
    return {
        'REST_ENDPOINT': 'http://localhost:9999',
        'RELPATH': '/pcdm',
        'LOG_DIR': '/logs',
        'AUTH_TOKEN': 'foobar'
    }


@pytest.fixture
def endpoint(repo_base_config):
    return Endpoint(
        url=repo_base_config['REST_ENDPOINT'],
        default_path=repo_base_config['RELPATH'],
    )


@pytest.fixture
def client(repo_base_config, endpoint) -> Client:
    return Client(
        endpoint=endpoint,
        auth=get_authenticator(repo_base_config),
    )
