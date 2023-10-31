import re
from typing import Callable
from uuid import uuid4

import pytest
from httpretty import httpretty
from rdflib import Graph

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
def simulate_repo() -> Callable[[Graph], None]:
    """Pytest fixture that uses HTTPretty to simulate a read-only repository.
    The repository is defined using a Graph. Each unique subject in that graph
    is assumed to be its own resource. Each resource will respond to HEAD and
    GET requests with 200 OK and Content-Type application/n-triples."""
    def _register_repo(graph: Graph):
        subjects = set(graph.subjects())
        for subject in subjects:
            resource_graph = Graph()
            for triple in graph.triples((subject, None, None)):
                resource_graph.add(triple)
            body = resource_graph.serialize(format='application/n-triples')
            httpretty.register_uri(
                method=httpretty.HEAD,
                uri=subject,
                status=200,
                adding_headers={
                    'Content-Type': 'application/n-triples',
                },
            )
            httpretty.register_uri(
                method=httpretty.GET,
                uri=subject,
                status=200,
                body=body,
                adding_headers={
                    'Content-Type': 'application/n-triples',
                },
            )
    return _register_repo


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
