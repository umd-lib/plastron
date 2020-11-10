import logging
import os

from stomp import Connection
from stomp.exception import ConnectFailedException
from time import sleep


logger = logging.getLogger(__name__)


class Message:
    @classmethod
    def read(cls, filename):
        headers = {}
        body = ''
        in_body = False
        with open(filename, 'r') as fh:
            for line in fh:
                if in_body:
                    body += line
                else:
                    if line.rstrip() == '':
                        in_body = True
                        continue
                    else:
                        (key, value) = line.split(':', 1)
                        headers[key] = value.strip()
        return cls(headers=headers, body=body)

    def __init__(self, headers=None, body=''):
        if headers is not None:
            self.headers = headers
        else:
            self.headers = {}
        self.body = body
        self.id = self.headers.get('message-id')

    def __str__(self):
        return '\n'.join([f'{k}: {v}' for k, v in self.headers.items()]) + '\n\n' + self.body


class PlastronMessage(Message):
    def __init__(self, headers=None, body=''):
        super().__init__(headers, body)
        self.job_id = self.headers['PlastronJobId']


class PlastronCommandMessage(PlastronMessage):
    def __init__(self, headers=None, body=''):
        super().__init__(headers, body)
        self.command = self.headers['PlastronCommand']
        self.args = {h[12:]: v for h, v in self.headers.items() if h.startswith('PlastronArg-')}


class MessageBox:
    def __init__(self, directory):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        # default to using a basic message class
        self.message_class = Message

    def add(self, id, message):
        filename = os.path.join(self.dir, id.replace('/', '-'))
        with open(filename, 'wb') as fh:
            fh.write(str(message).encode())

    def remove(self, id):
        filename = os.path.join(self.dir, id.replace('/', '-'))
        os.remove(filename)

    def __iter__(self):
        self.filenames = os.listdir(self.dir)
        self.index = 0
        self.count = len(self.filenames)
        return self

    def __next__(self):
        if self.index < self.count:
            filename = os.path.join(self.dir, self.filenames[self.index])
            self.index += 1
            return self.message_class.read(filename)
        else:
            raise StopIteration

    def __call__(self, message_class):
        self.message_class = message_class
        return iter(self)


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
