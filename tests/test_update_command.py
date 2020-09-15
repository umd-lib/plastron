from argparse import Namespace
from plastron.commands.update import Command
from plastron.exceptions import FailureException
from plastron.stomp import PlastronCommandMessage
from pytest import raises


def test_parse_message():
    message_body = '{\"uri\": [\"test\"], \"sparql_update\": \"\" }'

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


class TinyRepoMock:
    def test_connection(self):
        return True


def test_validate_requires_model():
    cmd = Command()
    args = Namespace(
        dry_run=False,
        use_transactions=False,
        validate=True,
        model=''
    )
    with raises(FailureException) as exc_info:
        cmd.execute(TinyRepoMock(), args)
    assert exc_info.value.args[0] == "Model must be provided when performing validation"
