from unittest.mock import MagicMock

import pytest
from pytest import raises

from plastron.client import Client, Endpoint
from plastron.models import Letter, Item
from plastron.repo import Repository
from plastron.stomp.commands.update import parse_message
from plastron.stomp.messages import PlastronCommandMessage


@pytest.fixture
def message_body():
    return '{"uris": ["test"], "sparql_update": "" }'


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
def client(endpoint):
    return Client(endpoint=endpoint)


@pytest.fixture
def mock_repo(client):
    return MagicMock(spec=Repository)


@pytest.mark.parametrize(
    ('headers', 'expected_args'),
    [
        (
            # headers
            {
                'PlastronJobId': 'test',
                'PlastronCommand': 'update',
                'PlastronArg-dry-run': 'True',
                'PlastronArg-validate': 'False',
                'PlastronArg-no-transactions': 'False'
            },
            # expected args
            {
                'uris': ['test'],
                'sparql_update': '',
                'model_class': None,
                'traverse': [],
                'dry_run': True,
                # Default to no transactions, due to LIBFCREPO-842
                'use_transactions': True,
            },
        ),
        (
            # headers
            {
                'PlastronJobId': 'test',
                'PlastronCommand': 'update',
                'PlastronArg-dry-run': 'False',
                'PlastronArg-validate': 'True',
                'PlastronArg-model': 'Item',
                'PlastronArg-no-transactions': 'False'
            },
            # expected_args
            {
                'uris': ['test'],
                'sparql_update': '',
                'model_class': Item,
                'traverse': [],
                'dry_run': False,
                # Default to no transactions, due to LIBFCREPO-842
                'use_transactions': True,
            },
        ),
        (
            # headers
            {
                'PlastronJobId': 'test',
                'PlastronCommand': 'update',
                'PlastronArg-dry-run': 'False',
                'PlastronArg-validate': 'False',
                'PlastronArg-no-transactions': 'True'
            },
            # expected_args
            {
                'uris': ['test'],
                'sparql_update': '',
                'model_class': None,
                'traverse': [],
                'dry_run': False,
                # Default to no transactions, due to LIBFCREPO-842
                'use_transactions': False,
            },
        ),
        (
            # headers
            {
                'PlastronJobId': 'test',
                'PlastronCommand': 'update',
                'PlastronArg-model': 'Letter',
            },
            # expected_args
            {
                'uris': ['test'],
                'sparql_update': '',
                'model_class': Letter,
                'traverse': [],
                'dry_run': False,
                # Default to no transactions, due to LIBFCREPO-842
                'use_transactions': False,
            },
        ),
    ],
)
def test_parse_message(message_body, headers, expected_args):
    message = PlastronCommandMessage(headers=headers, body=message_body)
    assert parse_message(message) == expected_args


def test_validate_requires_model(mock_repo, message_body):
    headers = {
        'PlastronJobId': 'test',
        'PlastronCommand': 'update',
        'PlastronArg-dry-run': 'False',
        'PlastronArg-validate': 'True',
        'PlastronArg-no-transactions': 'True'
    }
    message = PlastronCommandMessage(headers=headers, body=message_body)
    with raises(RuntimeError) as exc_info:
        parse_message(message)
    assert exc_info.value.args[0] == "Model must be provided when performing validation"
