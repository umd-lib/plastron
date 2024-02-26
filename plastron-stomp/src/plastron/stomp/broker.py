import logging
import os
from pathlib import Path
from typing import NamedTuple, Optional, Dict, Union

from stomp import Connection11
from stomp.exception import StompException

from plastron.stomp import __version__
from plastron.stomp.messages import Message

logger = logging.getLogger(__name__)


class ServerTuple(NamedTuple):
    host: str
    port: int

    @classmethod
    def from_string(cls, value: str) -> 'ServerTuple':
        host, port = value.split(':', 1)
        return cls(host=host, port=int(port))

    def __str__(self):
        return f'{self.host}:{self.port}'


class Broker:
    def __init__(
            self,
            server: ServerTuple,
            message_store_dir: Union[Path, str],
            destinations: Optional[Dict[str, str]] = None,
            public_uri_template: Optional[str] = None,
    ):
        self.server = server
        self.connection = Connection11([self.server])
        self.client_id = f'plastrond/{__version__}-{os.uname().nodename}-{os.getpid()}'
        self.destinations = {key.upper(): Destination(self, value) for key, value in destinations.items()}
        self.message_store_dir = message_store_dir
        self.public_uri_template = public_uri_template

    def __str__(self):
        return f'{self.server} (client-id: {self.client_id})'

    def __getitem__(self, item) -> 'Destination':
        return self.destination(item)

    def connect(self) -> bool:
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

    def disconnect(self):
        self.connection.disconnect()

    def set_listener(self, *args):
        self.connection.set_listener(*args)

    def subscribe(self, *args, **kwargs):
        self.connection.subscribe(*args, **kwargs)

    def ack(self, *args):
        self.connection.ack(*args)

    def destination(self, name: str) -> 'Destination':
        return self.destinations[name.upper()]

    def send(self, destination, headers=None, body='', **kwargs):
        if headers is None:
            headers = {}
        self.connection.send(
            destination=destination,
            headers=headers,
            body=body,
            **kwargs
        )


class Destination:
    def __init__(self, broker: Broker, destination: str):
        self.broker = broker
        self.name = destination

    def __str__(self):
        return self.name

    def send(self, message: Message):
        self.broker.connection.send(destination=self.name, headers=message.headers, body=message.body)

    def subscribe(self, id: str, ack: str = 'auto', headers: Dict = None, **kwargs):
        self.broker.connection.subscribe(destination=self.name, id=id, ack=ack, headers=headers, **kwargs)
        logger.info(f"Subscribed to {self.name}")
        logger.debug(f"id={id} ack={ack} headers={headers} {kwargs}")
