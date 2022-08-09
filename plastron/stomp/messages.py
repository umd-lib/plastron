import json
import logging
import os

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

    @classmethod
    def add_header_mapping(cls, header_name: str, attr_name: str):
        def getter(self):
            return self.headers[header_name]

        def setter(self, value):
            self.headers[header_name] = value

        def deleter(self):
            del self.headers[header_name]

        setattr(cls, attr_name, property(getter, setter, deleter))

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

        self.add_header_mapping('message-id', 'id')
        self.add_header_mapping('persistent', 'persistent')
        if message_id is not None:
            self.id = message_id
        if persistent is not None:
            self.persistent = persistent

    def __str__(self):
        return '\n'.join([f'{k}:{v}' for k, v in self.headers.items()]) + '\n\n' + self.body


class PlastronMessage(Message):
    def __init__(self, job_id: str = None, **kwargs):
        super().__init__(**kwargs)
        self.add_header_mapping('PlastronJobId', 'job_id')
        # Plastron message are persistent by default
        if 'persistent' not in self.headers:
            self.persistent = 'true'
        if job_id is not None:
            self.job_id = job_id


class PlastronResponseMessage(PlastronMessage):
    def __init__(self, state: str = None, **kwargs):
        super().__init__(**kwargs)
        self.add_header_mapping('PlastronJobState', 'state')
        if state is not None:
            self.state = state


class PlastronErrorMessage(PlastronMessage):
    def __init__(self, error: str = None, **kwargs):
        super().__init__(**kwargs)
        self.add_header_mapping('PlastronJobError', 'error')
        if error is not None:
            self.error = error


class PlastronCommandMessage(PlastronMessage):
    def __init__(self, command: str = None, args: dict = None, **kwargs):
        super().__init__(**kwargs)
        self.add_header_mapping('PlastronCommand', 'command')
        if command is not None:
            self.command = command
        if args is not None:
            for name, value in args.items():
                self.headers[f'PlastronArg-{name}'] = value

    @property
    def args(self):
        return {h[12:]: v for h, v in self.headers.items() if h.startswith('PlastronArg-')}

    def response(self, state: str, body) -> PlastronResponseMessage:
        """
        Return a new PlastronResponseMessage with the same job_id as this message,
        plus the given state and body.
        """
        return PlastronResponseMessage(job_id=self.job_id, state=state, body=body)


class MessageBox:
    def __init__(self, directory):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        # default to using a basic message class
        self.message_class = Message

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
            return self.message_class.read(filename)
        else:
            raise StopIteration

    def __call__(self, message_class):
        self.message_class = message_class
        return iter(self)
