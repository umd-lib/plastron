from importlib import import_module


class BaseCommand:
    def __init__(self, config=None):
        if config is None:
            config = {}
        self.config = config

    def repo_config(self, repo_config, args=None):
        """
        Enable default repository config dictionary to be overridden by the
        command before actually creating the repository.

        The default implementation of this method simply returns the provided
        repo_config dictionary without change
        """
        return repo_config


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
