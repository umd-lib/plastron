import json
import logging
import os

logger = logging.getLogger(__name__)


class MessageHeader:
    """
    Descriptor to map a STOMP message header name to a Python attribute.
    """
    def __init__(self, header_name: str):
        self.header_name = header_name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if not hasattr(instance, 'headers'):
            raise TypeError(f'Expected {instance} to have a "headers" attribute')
        return instance.headers.get(self.header_name)

    def __set__(self, instance, value):
        if not hasattr(instance, 'headers'):
            raise TypeError(f'Expected {instance} to have a "headers" attribute')
        instance.headers[self.header_name] = value

    def __delete__(self, instance):
        if not hasattr(instance, 'headers'):
            raise TypeError(f'Expected {instance} to have a "headers" attribute')
        if self.header_name in instance.headers:
            del instance.headers[self.header_name]


class Message:
    id = MessageHeader('message-id')
    persistent = MessageHeader('persistent')

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

    def __init__(self, message_id=None, persistent=None, headers=None, body=''):
        if headers is not None:
            self.headers = headers
        else:
            self.headers = {}

        if isinstance(body, dict):
            # convert dictionaries to JSON
            self.body = json.dumps(body)
        else:
            # treat all other bodies as strings
            self.body = str(body)

        if message_id is not None:
            self.id = message_id
        if persistent is not None:
            self.persistent = persistent

    def __str__(self):
        return '\n'.join([f'{k}:{v}' for k, v in self.headers.items()]) + '\n\n' + self.body


class PlastronMessage(Message):
    job_id = MessageHeader('PlastronJobId')

    def __init__(self, job_id: str = None, **kwargs):
        super().__init__(**kwargs)
        # Plastron message are persistent by default
        if 'persistent' not in self.headers:
            self.persistent = 'true'
        if job_id is not None:
            self.job_id = job_id


class PlastronResponseMessage(PlastronMessage):
    state = MessageHeader('PlastronJobState')
    status_url = MessageHeader('PlastronStatusURL')

    def __init__(self, state: str = None, status_url: str = None, **kwargs):
        super().__init__(**kwargs)
        if state is not None:
            self.state = state
        if status_url is not None:
            self.status_url = status_url


class PlastronErrorMessage(PlastronMessage):
    error = MessageHeader('PlastronJobError')

    def __init__(self, error: str = None, **kwargs):
        super().__init__(**kwargs)
        if error is not None:
            self.error = error


class PlastronCommandMessage(PlastronMessage):
    command = MessageHeader('PlastronCommand')
    status_url = MessageHeader('PlastronStatusURL')

    def __init__(self, command: str = None, status_url: str = None, args: dict = None, **kwargs):
        super().__init__(**kwargs)
        if command is not None:
            self.command = command
        if status_url is not None:
            self.status_url = status_url
        if args is not None:
            for name, value in args.items():
                self.headers[f'PlastronArg-{name}'] = value

    @property
    def args(self):
        return {h[12:]: v for h, v in self.headers.items() if h.startswith('PlastronArg-')}

    def response(self, state: str, body) -> PlastronResponseMessage:
        """
        Return a new PlastronResponseMessage with the same `job_id` and `status_url`
        as this message, plus the given state and body.
        """
        return PlastronResponseMessage(job_id=self.job_id, status_url=self.status_url, state=state, body=body)


class MessageBox:
    def __init__(self, directory, message_class=None):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        self.message_class = message_class

    def add(self, message_id, message):
        filename = os.path.join(self.dir, message_id.replace('/', '-'))
        with open(filename, 'wb') as fh:
            fh.write(str(message).encode())

    def remove(self, message_id):
        filename = os.path.join(self.dir, message_id.replace('/', '-'))
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
            if self.message_class is not None:
                return self.message_class.read(filename)
            else:
                # just return the filename if no message class was defined
                return filename
        else:
            raise StopIteration
