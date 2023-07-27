import importlib.metadata
import logging
import os

from stomp import Connection11
from stomp.exception import StompException

from plastron.stomp.messages import Message

logger = logging.getLogger(__name__)
version = importlib.metadata.version('plastron-legacy')


class Broker:
    def __init__(self, config):
        # set up STOMP client
        self.server = tuple(config['SERVER'].split(':', 2))
        self.connection = Connection11([self.server])
        self.client_id = f'plastrond/{version}-{os.uname().nodename}-{os.getpid()}'
        self.destinations = config.get('DESTINATIONS', {})
        self.message_store_dir = config['MESSAGE_STORE_DIR']
        self.public_uri_template = config.get('PUBLIC_URI_TEMPLATE', os.environ.get('PUBLIC_URI_TEMPLATE', None))

    def __str__(self):
        return f'{":".join(self.server)} (client-id: {self.client_id})'

    def connect(self):
        if not self.connection.is_connected():
            logger.info(
                f'Attempting to connect to STOMP message broker ('
                f'Host: {self.server[0]}, '
                f'Port: {self.server[1]}, '
                f'Client ID: {self.client_id})'
            )
            try:
                self.connection.connect(wait=True, headers={'client-id': self.client_id})
            except StompException:
                logger.error(f'STOMP connection failed for {self}')
                return False
        return self.connection.is_connected()

    def set_listener(self, *args):
        self.connection.set_listener(*args)

    def subscribe(self, *args, **kwargs):
        self.connection.subscribe(*args, **kwargs)

    def ack(self, *args):
        self.connection.ack(*args)

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

    def send(self, message: Message):
        self.broker.send_message(self.destination, headers=message.headers, body=message.body)
