import logging
import os

from stomp import Connection
from stomp.exception import ConnectFailedException
from time import sleep


logger = logging.getLogger(__name__)


class Broker:
    def __init__(self, config):
        # set up STOMP client
        broker_server = tuple(config['SERVER'].split(':', 2))
        self.connection = Connection([broker_server])
        self.destinations = config.get('DESTINATIONS', {})
        self.message_store_dir = config['MESSAGE_STORE_DIR']
        self.public_uri_template = config.get('PUBLIC_URI_TEMPLATE', os.environ.get('PUBLIC_URI_TEMPLATE', None))

    def connect(self):
        while not self.connection.is_connected():
            logger.info('Attempting to connect to the STOMP message broker')
            try:
                self.connection.connect(wait=True)
            except ConnectFailedException:
                logger.warning('Connection attempt failed')
                sleep(1)

    def destination(self, name: str):
        return self.destinations[name.upper()]

    def send_message(self, destination, headers=None, body='', **kwargs):
        if headers is None:
            headers = {}
        self.connection.send(
            destination=destination,
            headers=headers,
            body=body,
            **kwargs
        )

    def disconnect(self):
        self.connection.disconnect()


class Destination:
    def __init__(self, broker, destination):
        self.broker = broker
        self.destination = destination

    def __str__(self):
        return self.destination

    def send(self, **kwargs):
        self.broker.send_message(self.destination, **kwargs)
