import pytest

from plastron.client import Repository, Client, get_authenticator


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
def repo(repo_base_config):
    return Repository(
        endpoint=repo_base_config['REST_ENDPOINT'],
        default_path=repo_base_config['RELPATH'],
    )


@pytest.fixture
def client(repo_base_config, repo) -> Client:
    return Client(
        repo=repo,
        auth=get_authenticator(repo_base_config),
    )
