from importlib import import_module
from typing import Generator, Any

from plastron.context import PlastronContext


class BaseCommand:
    def __init__(self, context: PlastronContext = None):
        self.context = context
        self.result = None

    @property
    def config(self) -> dict[str, Any]:
        name = self.__module__.split('.')[-1]
        if name == 'importcommand':
            name = 'import'
        return self.context.config.get('COMMANDS', {}).get(name.upper(), {})

    def _run(self, command: Generator):
        # delegating generator; each progress step is passed to the calling
        # method, and the return value from the command is stored as the result
        self.result = yield from command

    def run(self, command: Generator):
        # Run the delegating generator to exhaustion, discarding the intermediate
        # yielded values. Return the final result.
        for _ in self._run(command):
            pass
        return self.result


def get_command_class(command_name: str):
    module_name = command_name
    if command_name == 'import':
        # special case for the import command, to avoid conflict
        # with the "import" keyword
        module_name += 'command'
    try:
        command_module = import_module('.'.join([__package__, module_name]))
    except ModuleNotFoundError as e:
        raise RuntimeError(f'Unable to load a command with the name {command_name}') from e
    command_class = getattr(command_module, 'Command')
    if command_class is None:
        raise RuntimeError(f'Command class not found in module {command_module}')

    return command_class
