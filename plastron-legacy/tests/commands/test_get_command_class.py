import pytest

from plastron.commands import BaseCommand, get_command_class
from plastron.exceptions import FailureException


def test_get_command_class():
    cls = get_command_class('create')
    assert issubclass(cls, BaseCommand)


def test_get_import_command_class():
    # test the special case
    cls = get_command_class('import')
    assert issubclass(cls, BaseCommand)
    assert cls.__module__ == 'plastron.commands.importcommand'


def test_non_existent_command_class():
    with pytest.raises(FailureException):
        _cls = get_command_class('foo')
