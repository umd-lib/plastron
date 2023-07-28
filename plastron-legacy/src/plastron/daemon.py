import importlib.metadata
import logging
import logging.config
import sys
from argparse import ArgumentParser, FileType
from datetime import datetime
from pathlib import Path
from threading import Event, Thread
from typing import Any, Mapping, Tuple, Type

import yaml

from plastron.core.util import envsubst, DEFAULT_LOGGING_OPTIONS
from plastron.stomp import Broker
from plastron.stomp.listeners import CommandListener

logger = logging.getLogger(__name__)
version = importlib.metadata.version('plastron-legacy')


class STOMPDaemon(Thread):
    def __init__(self, config=None, **kwargs):
        super().__init__(**kwargs)
        if config is None:
            config = {}
        self.config = config
        self.started = Event()
        self.stopped = Event()
        # configure STOMP message broker
        self.broker = Broker(self.config['MESSAGE_BROKER'])
        self.command_listener = CommandListener(self)

    def run(self):
        # setup listeners
        self.broker.set_listener('command', self.command_listener)

        # connect and listen until the stopped Event is set
        if self.broker.connect():
            self.stopped.wait()

            self.broker.disconnect()
            if self.command_listener.inbox_watcher:
                self.command_listener.inbox_watcher.stop()
        else:
            logger.error('Unable to connect to STOMP broker')


INTERFACES = {
    'stomp': STOMPDaemon,
}


def configure_logging(log_filename_base: str, log_dir: str = 'logs', verbose: bool = False) -> None:
    logging_options = DEFAULT_LOGGING_OPTIONS

    # log file configuration
    log_dirname = Path(log_dir)
    log_dirname.mkdir(parents=True, exist_ok=True)
    now = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    log_filename = '.'.join((log_filename_base, now, 'log'))
    logfile = log_dirname / log_filename
    logging_options['handlers']['file']['filename'] = str(logfile)

    # manipulate console verbosity
    if verbose:
        logging_options['handlers']['console']['level'] = 'DEBUG'

    # configure logging
    logging.config.dictConfig(logging_options)


def get_daemon_config() -> Tuple[Type[Any], Mapping[str, Any]]:
    parser = ArgumentParser(
        prog='plastrond',
        description='Batch operations daemon for Fedora 4.'
    )
    parser.add_argument(
        '-c', '--config',
        dest='config_file',
        help='path to configuration file',
        type=FileType(),
        action='store',
        required=True
    )
    parser.add_argument(
        '-v', '--verbose',
        help='increase the verbosity of the status output',
        action='store_true'
    )
    parser.add_argument(
        'interface',
        help='interface to run',
        choices=INTERFACES.keys()
    )

    # parse command line args
    args = parser.parse_args()

    config = envsubst(yaml.safe_load(args.config_file))

    configure_logging(
        log_filename_base='plastron.daemon',
        log_dir=config.get('REPOSITORY', {}).get('LOG_DIR', 'logs'),
        verbose=args.verbose
    )

    return INTERFACES[args.interface], config


def main():
    daemon_class, config = get_daemon_config()
    daemon_description = f'plastrond/{version} ({daemon_class.__name__})'

    logger.info(f'Starting {daemon_description}')

    thread = daemon_class(config=config)

    try:
        thread.start()
        while thread.is_alive():
            thread.join(1)
    except KeyboardInterrupt:
        logger.warning(f'Shutting down {daemon_description}')
        if hasattr(thread, 'stopped'):
            thread.stopped.set()
            thread.stopped.wait()

    logger.info(f'{daemon_description} shut down successfully')
    sys.exit()


if __name__ == "__main__":
    main()
