#!/usr/bin/env python3
import argparse
import logging
import logging.config
import os
import signal
import sys
from pathlib import Path
from threading import Thread

import waitress
import yaml

from datetime import datetime
from plastron import version
from plastron.logging import DEFAULT_LOGGING_OPTIONS
from plastron.stomp import Broker
from plastron.stomp.listeners import ReconnectListener, CommandListener
from plastron.util import envsubst
from plastron.web import create_app

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

    logger.info(f'plastrond {version}')

    threads = [
        STOMPDaemon(config=config),
        HTTPDaemon(config=config)
    ]

    try:
        # start all daemons
        for thread in threads:
            thread.start()
        # block until all threads have exited
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        # exit on Ctrl+C
        sys.exit()


class STOMPDaemon(Thread):
    def __init__(self, config=None, **kwargs):
        super().__init__(daemon=True, **kwargs)
        if config is None:
            config = {}
        self.repo_config = config['REPOSITORY']
        self.broker_config = config['MESSAGE_BROKER']
        self.command_config = config.get('COMMANDS', {})

    def run(self):
        # configure STOMP message broker
        broker = Broker(self.broker_config)

        # setup listeners
        # Order of listeners is important -- ReconnectListener should be the
        # last listener
        broker.set_listener('command', CommandListener(broker, self.repo_config, self.command_config))
        broker.set_listener('reconnect', ReconnectListener(broker))

        # connect and listen indefinitely
        broker.connect()
        while True:
            signal.pause()


class HTTPDaemon(Thread):
    def __init__(self, config=None, **kwargs):
        super().__init__(daemon=True, **kwargs)
        self.jobs_dir = config.get('COMMANDS', {}).get('IMPORT', {}).get('JOBS_DIR', 'jobs')
        server_config = config.get('HTTP_SERVER', {})
        self.host = server_config.get('HOST', '0.0.0.0')
        self.port = int(server_config.get('PORT', 5000))

    def run(self):
        app = create_app({'JOBS_DIR': Path(self.jobs_dir)})
        logger.info(f'HTTP server listening on {self.host}:{self.port}')
        waitress.serve(app, host=self.host, port=self.port)


if __name__ == "__main__":
    main()
