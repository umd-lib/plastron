from importlib import import_module
from types import ModuleType


def get_module_name(command_name: str) -> str:
    if command_name == 'import':
        # special case for the import command, to avoid conflict
        # with the "import" keyword
        return command_name + 'command'
    else:
        return command_name


def get_command_module(command_name: str) -> ModuleType:
    try:
        return import_module(".".join((__package__, get_module_name(command_name))))
    except ModuleNotFoundError as e:
        raise RuntimeError(f'Unable to load a command with the name "{command_name}"') from e
