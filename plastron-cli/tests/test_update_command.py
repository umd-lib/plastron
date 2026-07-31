from argparse import Namespace
from unittest.mock import MagicMock

from pytest import raises

from plastron.cli.commands.update import Command
from plastron.client import Client
from plastron.context import PlastronContext


def test_validate_requires_model():
    mock_context = MagicMock(spec=PlastronContext, client=MagicMock(spec=Client))
    cmd = Command(
        context=mock_context
    )
    args = Namespace(
        dry_run=False,
        use_transactions=False,
        validate=True,
        model=None
    )
    with raises(RuntimeError) as exc_info:
        cmd.__call__(args)
    assert exc_info.value.args[0] == "Model must be provided when performing validation"
