from argparse import ArgumentParser
from plastron.cli import load_commands
from plastron.cli.commands import importcommand


def test_find_commands_correctly_handles_import_command():
    # The "import" command is special, because the name of the command
    # is "import", but the module is "importcommand"
    parser = ArgumentParser(
        prog='plastron',
        description='Batch operation tool for Fedora 4.'
    )
    parser.set_defaults(cmd_name=None)

    subparsers = parser.add_subparsers(title='commands')

    command_modules = load_commands(subparsers)
    assert "import" in command_modules
    assert command_modules["import"] == importcommand
