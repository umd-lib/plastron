import logging
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path
from threading import Event, Thread
from typing import TextIO, Dict, Any, Optional

import click
import yaml

from plastron.core.util import envsubst, DEFAULT_LOGGING_OPTIONS
from plastron.stomp import __version__
from plastron.stomp.broker import ServerTuple, Broker
from plastron.stomp.listeners import CommandListener

logger = logging.getLogger(__name__)


class STOMPDaemon(Thread):
    def __init__(
            self,
            broker: Broker,
            repo_config: Dict[str, Any],
            command_config: Optional[Dict[str, Any]] = None,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.started = Event()
        self.stopped = Event()
        self.broker = broker

        def started():
            self.stopped.clear()
            self.started.set()

        def stopped():
            self.started.clear()
            self.stopped.set()

        self.command_listener = CommandListener(
            broker=self.broker,
            repo_config=repo_config,
            command_config=command_config,
            after_connected=started,
            after_disconnected=stopped,
        )

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


@click.command
@click.option(
    '-c', '--config-file',
    type=click.File(),
    help='Configuration file',
    required=True,
)
@click.option(
    '-v', '--verbose',
    is_flag=True,
    help='increase the verbosity of the status output',
)
def main(config_file: TextIO, verbose: bool):
    config = envsubst(yaml.safe_load(config_file))
    repo_config = config.get('REPOSITORY', {})
    command_config = config.get('COMMANDS', {})
    broker_config = config['MESSAGE_BROKER']
    configure_logging(
        log_filename_base='plastron.daemon',
        log_dir=repo_config.get('LOG_DIR', 'logs'),
        verbose=verbose,
    )
    daemon_description = f'plastrond-stomp/{__version__}'

    logger.info(f'Starting {daemon_description}')
    broker = Broker(
        server=ServerTuple.from_string(broker_config['SERVER']),
        message_store_dir=broker_config['MESSAGE_STORE_DIR'],
        destinations=broker_config.get('DESTINATIONS'),
        public_uri_template=broker_config.get('PUBLIC_URI_TEMPLATE', os.environ.get('PUBLIC_URI_TEMPLATE', None))
    )

    thread = STOMPDaemon(broker=broker, repo_config=repo_config, command_config=command_config)

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
