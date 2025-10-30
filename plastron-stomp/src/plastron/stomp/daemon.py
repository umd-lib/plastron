import logging
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path
from threading import Event, Thread
from typing import TextIO, Any

import click
import yaml

from plastron.context import PlastronContext
from plastron.stomp import __version__
from plastron.stomp.listeners import CommandListener
from plastron.utils import DEFAULT_LOGGING_OPTIONS, envsubst

logger = logging.getLogger(__name__)


class STOMPDaemon(Thread):
    def __init__(self, config: dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.context = PlastronContext(config)
        self.started = Event()
        self.stopped = Event()
        self.broker = self.context.broker

        def started():
            self.stopped.clear()
            self.started.set()

        def stopped():
            self.started.clear()
            self.stopped.set()

        self.command_listener = CommandListener(
            context=self.context,
            after_connected=started,
            after_disconnected=stopped,
        )

    def run(self):
        # setup listeners
        self.broker.set_listener('command', self.command_listener)

        # connect and listen until the stopped Event is set
        if self.broker.connect(client_id=f'plastrond/{__version__}-{os.uname().nodename}-{os.getpid()}'):
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
    configure_logging(
        log_filename_base='plastron.daemon',
        log_dir=repo_config.get('LOG_DIR', 'logs'),
        verbose=verbose,
    )
    daemon_description = f'plastrond-stomp/{__version__}'
    logger.info(f'Starting {daemon_description}')
    thread = STOMPDaemon(config=config)

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
