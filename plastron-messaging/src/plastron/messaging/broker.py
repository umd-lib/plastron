import logging
from pathlib import Path
from typing import NamedTuple, Optional

from stomp import Connection11
from stomp.exception import StompException
from stomp.transport import Transport

from plastron.messaging.messages import Message

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


class HeartbeatTuple(NamedTuple):
    send: int = 0
    receive: int = 0

    @classmethod
    def from_dict(cls, value: dict) -> 'HeartbeatTuple':
        return HeartbeatTuple(**{k.lower(): v for k, v in value.items()})


class Broker:
    def __init__(
        self,
        server: ServerTuple,
        message_store_dir: Path | str,
        destinations: Optional[dict[str, str]] = None,
        public_uri_template: Optional[str] = None,
        heartbeat: HeartbeatTuple = None,
    ):
        self.server = server
        self.connection = Connection11([self.server])
        self.destinations = {key.upper(): Destination(self, value) for key, value in destinations.items()}
        self.message_store_dir = message_store_dir
        self.public_uri_template = public_uri_template
        self.heartbeat = heartbeat
        self.client_id = None

    def __str__(self):
        return f'{self.server} (client-id: {self.client_id})'

    def __getitem__(self, item) -> 'Destination':
        return self.destination(item)

    @property
    def transport(self) -> Transport:
        return self.connection.transport

    def connect(self, client_id: str) -> bool:
        if not self.connection.is_connected():
            logger.info(
                f'Attempting to connect to STOMP message broker ('
                f'Host: {self.server.host}, '
                f'Port: {self.server.port}, '
                f'Client ID: {client_id})'
            )
            try:
                self.connection.connect(wait=True, headers={'client-id': client_id})
            except StompException:
                logger.error(f'STOMP connection failed for {self}')
                return False
            else:
                self.client_id = client_id
        return self.connection.is_connected()

    def disconnect(self):
        self.connection.disconnect()
        self.client_id = None

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
        logger.debug(f'Sending message to {self.name}')
        logger.debug(f'Message headers: {message.headers}')
        self.broker.connection.send(destination=self.name, headers=message.headers, body=message.body)

    def subscribe(self, id: str, ack: str = 'auto', headers: dict = None, **kwargs):
        self.broker.connection.subscribe(destination=self.name, id=id, ack=ack, headers=headers, **kwargs)
        logger.info(f"Subscribed to {self.name}")
        logger.debug(f"id={id} ack={ack} headers={headers} {kwargs}")
