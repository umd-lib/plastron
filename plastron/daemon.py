#!/usr/bin/env python3
import argparse
import logging
import logging.config
import os
import signal
import sys
import yaml

from datetime import datetime
from plastron import version
from plastron.logging import DEFAULT_LOGGING_OPTIONS
from plastron.stomp import Broker
from plastron.stomp.listeners import ReconnectListener, CommandListener
from plastron.util import envsubst

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')


def main():
    parser = argparse.ArgumentParser(
        prog='plastron',
        description='Batch operations daemon for Fedora 4.'
    )
    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file.',
        action='store',
        required=True
    )
    parser.add_argument(
        '-v', '--verbose',
        help='increase the verbosity of the status output',
        action='store_true'
    )

    # parse command line args
    args = parser.parse_args()

    with open(args.config, 'r') as config_file:
        config = envsubst(yaml.safe_load(config_file))

    repo_config = config['REPOSITORY']
    broker_config = config['MESSAGE_BROKER']
    command_config = config.get('COMMANDS', {})

    logging_options = DEFAULT_LOGGING_OPTIONS

    # log file configuration
    log_dirname = repo_config.get('LOG_DIR')
    if not os.path.isdir(log_dirname):
        os.makedirs(log_dirname)
    log_filename = f'plastron.daemon.{now}.log'
    logfile = os.path.join(log_dirname, log_filename)
    logging_options['handlers']['file']['filename'] = logfile

    # manipulate console verbosity
    if args.verbose:
        logging_options['handlers']['console']['level'] = 'DEBUG'

    # configure logging
    logging.config.dictConfig(logging_options)

    # configure STOMP message broker
    broker = Broker(broker_config)

    logger.info(f'plastrond {version}')

    # setup listeners
    # Order of listeners is important -- ReconnectListener should be the
    # last listener
    broker.connection.set_listener('command', CommandListener(broker, repo_config, command_config))
    broker.connection.set_listener('reconnect', ReconnectListener(broker))

    try:
        broker.connect()
        while True:
            signal.pause()
    except KeyboardInterrupt:
        sys.exit()


if __name__ == "__main__":
    main()
