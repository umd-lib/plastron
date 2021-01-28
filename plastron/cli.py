#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import logging.config
import os
import sys
import yaml
from argparse import ArgumentParser, FileType
from datetime import datetime
from importlib import import_module
from pkgutil import iter_modules
from plastron import commands, version
from plastron.exceptions import FailureException
from plastron.logging import DEFAULT_LOGGING_OPTIONS
from plastron.http import Repository
from plastron.stomp import Broker

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')


def main():
    """Parse args and handle options."""

    parser = ArgumentParser(
        prog='plastron',
        description='Batch operation tool for Fedora 4.'
    )
    parser.set_defaults(cmd_name=None)

    common_required = parser.add_mutually_exclusive_group(required=True)
    common_required.add_argument(
        '-r', '--repo',
        help='Path to repository configuration file.',
        action='store'
    )
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

    subparsers = parser.add_subparsers(title='commands')

    # load all defined subcommands from the plastron.commands package
    command_modules = {}
    for finder, name, ispkg in iter_modules(commands.__path__):
        module = import_module(commands.__name__ + '.' + name)
        if hasattr(module, 'configure_cli'):
            module.configure_cli(subparsers)
            command_modules[name] = module

    # parse command line args
    args = parser.parse_args()

    # if no subcommand was selected, display the help
    if args.cmd_name is None:
        parser.print_help()
        sys.exit(0)

    if args.config_file is not None:
        # new-style, combined config file (a la plastron.daemon)
        config = yaml.safe_load(args.config_file)
        repo_config = config['REPOSITORY']
        broker_config = config.get('MESSAGE_BROKER', None)
        command_config = config.get('COMMANDS', {})
    else:
        # old-style, repository-only config file
        with open(args.repo, 'r') as repo_config_file:
            repo_config = yaml.safe_load(repo_config_file)
        broker_config = None
        command_config = {}

    fcrepo = Repository(
        repo_config, ua_string=f'plastron/{version}', on_behalf_of=args.delegated_user
    )

    if broker_config is not None:
        broker = Broker(broker_config)
    else:
        broker = None

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

    try:
        if hasattr(command_module, 'Command'):
            command = command_module.Command(config=command_config.get(args.cmd_name.upper()))
            command.repo = fcrepo
            command.broker = broker
        else:
            raise FailureException(f'Unable to execute command {args.cmd_name}')
        # dispatch to the selected subcommand
        print_header(args)
        logger.info(f'Loaded repo configuration from {args.repo or args.config_file.name}')
        if args.delegated_user is not None:
            logger.info(f'Running repository operations on behalf of {args.delegated_user}')
        command(fcrepo, args)
        print_footer(args)
    except FailureException as e:
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
