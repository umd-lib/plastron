from importlib import import_module


def get_command_class(command_name: str):
    module_name = command_name
    if command_name == 'import':
        # special case for the import command, to avoid conflict
        # with the "import" keyword
        module_name += 'command'
    try:
        command_module = import_module(".".join((__package__, module_name)))
    except ModuleNotFoundError as e:
        raise RuntimeError(f'Unable to load a command with the name "{command_name}"') from e
    command_class = getattr(command_module, 'Command')
    if command_class is None:
        raise RuntimeError(f'Command class not found in module "{command_module}"')

    return command_class
