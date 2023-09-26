import re
from uuid import uuid4

import pytest
from httpretty import httpretty

from plastron.client import Endpoint, Client
from plastron.client.auth import get_authenticator
from plastron.repo import Repository


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


@pytest.fixture
def repo(client) -> Repository:
    return Repository(client=client)


@pytest.fixture
def register_root(endpoint: Endpoint):
    def _register_root(status: int = 200):
        httpretty.register_uri(
            uri=endpoint.url.with_path('/'),
            method=httpretty.HEAD,
            status=status,
        )
    return _register_root


@pytest.fixture
def register_transaction(register_root, endpoint):
    def _register_transaction():
        register_root()
        txn_id = str(uuid4())
        txn_url = endpoint.url.with_path(f'/tx:{txn_id}')
        # creating a new transaction
        httpretty.register_uri(
            uri=endpoint.transaction_endpoint,
            method=httpretty.POST,
            status=201,
            adding_headers={
                'Location': txn_url,
            }
        )
        # maintenance action
        httpretty.register_uri(
            uri=txn_url,
            method=httpretty.POST,
            status=204,
        )
        # commit, and rollback actions
        httpretty.register_uri(
            uri=re.compile(txn_url + '/fcr:tx/fcr:(commit|rollback)'),
            method=httpretty.POST,
            status=204,
        )
        return txn_url
    return _register_transaction
