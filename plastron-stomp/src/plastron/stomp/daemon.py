import logging
import os
import sys
from threading import Event, Thread
from typing import TextIO, Any

import click
import yaml
from stomp.listener import HeartbeatListener

from plastron.context import PlastronContext
from plastron.stomp import __version__
from plastron.stomp.listeners import CommandListener
from plastron.utils import envsubst

logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'INFO').upper(),
    format='%(levelname)s:%(threadName)s:%(name)s:%(message)s',
)
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
        if self.broker.heartbeat is not None:
            logger.info(
                'Starting heartbeat; '
                f'Send interval: {self.broker.heartbeat.send}ms; '
                f'Receive interval: {self.broker.heartbeat.receive}ms'
            )
            self.broker.set_listener('heartbeat', HeartbeatListener(self.broker.transport, self.broker.heartbeat))
        # connect and listen until the stopped Event is set
        if self.broker.connect(client_id=f'plastrond/{__version__}-{os.uname().nodename}-{os.getpid()}'):
            self.stopped.wait()

            self.broker.disconnect()
            if self.command_listener.inbox_watcher:
                self.command_listener.inbox_watcher.stop()
        else:
            logger.error('Unable to connect to STOMP broker')


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
    deprecated='Set an explicit log level using the "LOG_LEVEL" environment variable.',
)
def main(config_file: TextIO, verbose: bool):
    if verbose:
        # set the root logger level
        logging.getLogger().setLevel(logging.DEBUG)

    config = envsubst(yaml.safe_load(config_file))
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
