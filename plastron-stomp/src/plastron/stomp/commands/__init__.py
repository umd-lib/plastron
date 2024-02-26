from importlib import import_module
from types import ModuleType
from typing import Optional, Dict, TypeVar, Type

from plastron.repo import Repository


class BaseCommand:
    def __init__(self, config=None):
        if config is None:
            config = {}
        self.config = config
        self.repo: Optional[Repository] = None
        self.result: Optional[Dict] = None


T = TypeVar('T', bound=BaseCommand)


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


def get_command_class(command_name: str) -> Type[BaseCommand]:
    command_module = get_command_module(command_name)
    command_class = getattr(command_module, 'Command')
    if command_class is None:
        raise RuntimeError(f'Command class not found in module "{command_module}"')

    return command_class
