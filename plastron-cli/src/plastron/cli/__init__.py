#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import importlib.metadata
import logging
import logging.config
import os
import sys
from argparse import ArgumentParser, FileType, Namespace
from datetime import datetime
from importlib import import_module
from pkgutil import iter_modules
from typing import Iterable

import yaml

from plastron.cli import commands
from plastron.cli.context import PlastronContext
from plastron.utils import DEFAULT_LOGGING_OPTIONS, envsubst

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')
version = importlib.metadata.version('plastron-cli')


def load_commands(subparsers):
    # load all defined subcommands from the plastron.commands package, using
    # introspection
    command_modules = {}
    for finder, name, ispkg in iter_modules(commands.__path__):
        module_name = name
        if module_name == "importcommand":
            # Special case handling for "importcommand", because "import" is
            # a Python reserved word that is not usable as a module name,
            # while we want "import" to be the Plastron command
            name = "import"

        module = import_module(commands.__name__ + '.' + module_name)
        if hasattr(module, 'configure_cli'):
            module.configure_cli(subparsers)
            command_modules[name] = module
    return command_modules


def get_uris(args: Namespace) -> Iterable[str]:
    if hasattr(args, 'uris_file') or hasattr(args, 'uris'):
        if hasattr(args, 'uris_file') and args.uris_file is not None:
            yield from (line.rstrip() for line in args.uris_file)
        if hasattr(args, 'uris') and args.uris is not None:
            yield from args.uris
    else:
        # fall back to STDIN
        yield from (line.rstrip() for line in sys.stdin)


def main():
    """Parse args and handle options."""

    parser = ArgumentParser(
        prog='plastron',
        description='Batch operation tool for Fedora 4.'
    )
    parser.set_defaults(cmd_name=None)

    common_required = parser.add_mutually_exclusive_group(required=True)
    common_required.add_argument(
        '-c', '--config',
        help='Path to configuration file.',
        action='store',
        dest='config_file',
        type=FileType('r')
    )
    common_required.add_argument(
        '-V', '--version',
        help='Print version and exit.',
        action='version',
        version=version
    )

    parser.add_argument(
        '-v', '--verbose',
        help='increase the verbosity of the status output',
        action='store_true'
    )
    parser.add_argument(
        '-q', '--quiet',
        help='decrease the verbosity of the status output',
        action='store_true'
    )
    parser.add_argument(
        '--on-behalf-of',
        help='delegate repository operations to this username',
        dest='delegated_user',
        action='store'
    )

    parser.add_argument(
        '--batch-mode', '-b',
        help='specifies the use of batch user for interaction with fcrepo',
        dest='batch_mode',
        action='store',
        default=False
    )

    subparsers = parser.add_subparsers(title='commands')

    command_modules = load_commands(subparsers)

    # parse command line args
    args = parser.parse_args()

    # if no subcommand was selected, display the help
    if args.cmd_name is None:
        parser.print_help()
        sys.exit(0)

    # new-style, combined config file (a la plastron.daemon)
    config = envsubst(yaml.safe_load(args.config_file))
    plastron_context = PlastronContext(config=config, args=args)
    repo_config: dict = config['REPOSITORY']

    # TODO: put these into their own "LOGGING" config section
    # get basic logging options
    if 'LOGGING_CONFIG' in repo_config:
        with open(repo_config.get('LOGGING_CONFIG'), 'r') as logging_config_file:
            logging_options = yaml.safe_load(logging_config_file)
    else:
        logging_options = DEFAULT_LOGGING_OPTIONS

    # log file configuration
    log_dirname = repo_config.get('LOG_DIR')
    if not os.path.isdir(log_dirname):
        os.makedirs(log_dirname)
    log_filename = 'plastron.{0}.{1}.log'.format(args.cmd_name, now)
    logfile = os.path.join(log_dirname, log_filename)
    logging_options['handlers']['file']['filename'] = logfile

    # manipulate console verbosity
    if args.verbose:
        logging_options['handlers']['console']['level'] = 'DEBUG'
    elif args.quiet:
        logging_options['handlers']['console']['level'] = 'WARNING'

    # configure logging
    logging.config.dictConfig(logging_options)

    # get the selected subcommand
    command_module = command_modules[args.cmd_name]

    # dispatch to the selected subcommand
    try:
        if not hasattr(command_module, 'Command'):
            raise RuntimeError(f'Unable to execute command {args.cmd_name}')

        command = command_module.Command(context=plastron_context)

        print_header(args)
        logger.info(f'Loaded repo configuration from {args.config_file.name}')
        if args.delegated_user is not None:
            logger.info(f'Running repository operations on behalf of {args.delegated_user}')
        command(args)
        print_footer(args)
    except RuntimeError as e:
        # something failed, exit with non-zero status
        logger.error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        # aborted due to Ctrl+C
        sys.exit(2)


def print_header(args):
    """Common header formatting."""
    if not args.quiet:
        title = '|     PLASTRON     |'
        bar = '+' + '=' * (len(title) - 2) + '+'
        spacer = '|' + ' ' * (len(title) - 2) + '|'
        print('\n'.join(['', bar, spacer, title, spacer, bar, '']), file=sys.stderr)


def print_footer(args):
    """Report success or failure and resources created."""
    if not args.quiet:
        print('\nScript complete. Goodbye!\n', file=sys.stderr)


if __name__ == "__main__":
    main()


class ConfigError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message
