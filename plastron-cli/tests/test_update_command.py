from io import StringIO
from argparse import Namespace
from unittest.mock import MagicMock

import pytest

from plastron.client import Client
from plastron.cli.commands.update import Command
from plastron.models import Letter, Item
from plastron.repo import Repository
from pytest import raises


def test_validate_requires_model():
    cmd = Command()
    args = Namespace(
        dry_run=False,
        use_transactions=False,
        validate=True,
        model=None
    )
    mock_repo = MagicMock(spec=Client)
    with raises(RuntimeError) as exc_info:
        cmd.__call__(mock_repo, args)
    assert exc_info.value.args[0] == "Model must be provided when performing validation"
