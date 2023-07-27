from argparse import Namespace
from unittest.mock import MagicMock

import pytest

from plastron.client import Client
from plastron.commands.update import Command
from plastron.core.exceptions import FailureException
from plastron.models import Letter, Item
from plastron.stomp.messages import PlastronCommandMessage
from pytest import raises


@pytest.fixture()
def message_body():
    return '{"uris": ["test"], "sparql_update": "" }'


def test_parse_message(message_body):
    headers = {
        'PlastronJobId': 'test',
        'PlastronCommand': 'update',
        'PlastronArg-dry-run': 'True',
        'PlastronArg-validate': 'False',
        'PlastronArg-no-transactions': 'False'
    }
    message = PlastronCommandMessage(headers=headers, body=message_body)
    namespace = Command.parse_message(message)

    assert (namespace.dry_run is True)
    assert (namespace.validate is False)
    assert (namespace.use_transactions is True)  # Opposite of value in header

    headers = {
        'PlastronJobId': 'test',
        'PlastronCommand': 'update',
        'PlastronArg-dry-run': 'False',
        'PlastronArg-validate': 'True',
        'PlastronArg-no-transactions': 'False'
    }
    message = PlastronCommandMessage(headers=headers, body=message_body)
    namespace = Command.parse_message(message)

    assert (namespace.dry_run is False)
    assert (namespace.validate is True)
    assert (namespace.use_transactions is True)  # Opposite of value in header

    headers = {
        'PlastronJobId': 'test',
        'PlastronCommand': 'update',
        'PlastronArg-dry-run': 'False',
        'PlastronArg-validate': 'False',
        'PlastronArg-no-transactions': 'True'
    }
    message = PlastronCommandMessage(headers=headers, body=message_body)
    namespace = Command.parse_message(message)

    assert (namespace.dry_run is False)
    assert (namespace.validate is False)
    assert (namespace.use_transactions is False)  # Opposite of value in header


def test_parse_message_model(message_body, repo):
    headers = {
        'PlastronJobId': 'test',
        'PlastronCommand': 'update',
        'PlastronArg-model': 'Letter',
    }
    message = PlastronCommandMessage(headers=headers, body=message_body)
    namespace = Command.parse_message(message)

    assert namespace.model == 'Letter'

    mock_client = MagicMock(spec=Client, repo=repo)
    cmd = Command()
    cmd.execute(mock_client, namespace)
    assert cmd.model_class is Letter


def test_model_class_loaded_on_each_execution(message_body, repo):
    """
    Testing the case where we have a single command instance, but execute() is
    run multiple times with different content models. The expected behavior is
    to pick the correct content model class each time.

    See https://umd-dit.atlassian.net/browse/LIBFCREPO-1121
    """
    cmd = Command()
    mock_client = MagicMock(spec=Client, repo=repo)

    headers = {
        'PlastronJobId': 'test',
        'PlastronCommand': 'update',
        'PlastronArg-model': 'Letter',
    }
    message = PlastronCommandMessage(headers=headers, body=message_body)
    namespace = Command.parse_message(message)
    assert namespace.model == 'Letter'

    cmd.execute(mock_client, namespace)
    assert cmd.model_class is Letter

    headers = {
        'PlastronJobId': 'test',
        'PlastronCommand': 'update',
        'PlastronArg-model': 'Item',
    }
    message = PlastronCommandMessage(headers=headers, body=message_body)
    namespace = Command.parse_message(message)
    assert namespace.model == 'Item'

    cmd.execute(mock_client, namespace)
    assert cmd.model_class is Item


def test_validate_requires_model():
    cmd = Command()
    args = Namespace(
        dry_run=False,
        use_transactions=False,
        validate=True,
        model=''
    )
    mock_repo = MagicMock(spec=Client)
    with raises(FailureException) as exc_info:
        cmd.execute(mock_repo, args)
    assert exc_info.value.args[0] == "Model must be provided when performing validation"
