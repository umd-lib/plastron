#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function

import argparse
from importlib import import_module
import os.path
import sys
import yaml
import logging
import logging.config
from datetime import datetime
from plastron.pcdm import Repository
from plastron.exceptions import FailureException
from plastron import commands, version

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')

#============================================================================
# MAIN LOOP
#============================================================================

def main():
    '''Parse args and handle options.'''

    parser = argparse.ArgumentParser(
            prog='plastron',
            description='Batch operation tool for Fedora 4.'
            )
    parser.set_defaults(cmd_name=None)

    common_required = parser.add_mutually_exclusive_group(required=True)
    common_required.add_argument('-r', '--repo',
            help='Path to repository configuration file.',
            action='store'
            )
    common_required.add_argument('-V', '--version',
            help='Print version and exit.',
            action='store_true'
            )

    parser.add_argument('-v', '--verbose',
            help='increase the verbosity of the status output',
            action='store_true'
            )
    parser.add_argument('-q', '--quiet',
            help='decrease the verbosity of the status output',
            action='store_true'
            )

    subparsers = parser.add_subparsers(title='commands')

    # load all defined subcommands
    command_for = {}
    for name in commands.__all__:
        command_module = import_module('plastron.commands.' + name)
        command_for[name] = command_module.Command(subparsers)

    # parse command line args
    args = parser.parse_args()

    # version check and exit
    if args.version:
        print(version)
        sys.exit(0)

    # if no subcommand was selected, display the help
    if args.cmd_name is None:
        parser.print_help()
        sys.exit(0)

    # load required repository config file and create repository object
    with open(args.repo, 'r') as repo_config_file:
        repo_config = yaml.safe_load(repo_config_file)
        fcrepo = Repository(repo_config)

    # get basic logging options
    with open('config/logging.yml', 'r') as configfile:
        logging_options = yaml.safe_load(configfile)

    # log file configuration
    log_dirname = repo_config.get('LOG_DIR')
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

    logger.info('Loaded repo configuration from {0}'.format(args.repo))

    # get the selected subcommand
    command = command_for[args.cmd_name]

    try:
        # dispatch to the selected subcommand
        command(fcrepo, args)
    except FailureException as e:
        # something failed, exit with non-zero status
        sys.exit(1)
    except KeyboardInterrupt as e:
        # aborted due to Ctrl+C
        sys.exit(2)

if __name__ == "__main__":
    main()
