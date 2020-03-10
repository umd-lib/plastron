import json
import logging

DEFAULT_LOGGING_OPTIONS = {
    'version': 1,
    'formatters': {
        'full': {
            'format': '%(levelname)s|%(asctime)s|%(threadName)s|%(name)s|%(message)s'
        },
        'messageonly': {
            'format': '%(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'messageonly',
            'stream': 'ext://sys.stderr'
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'full'
        }
    },
    'loggers': {
        '__main__': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
            'propagate': False
        },
        'plastron': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
            'propagate': False
        },
        # suppress logging output from paramiko by default
        'paramiko': {
            'propagate': False
        }
    },
    'root': {
        'level': 'DEBUG'
    }
}


STATUS_LOGGER = logging.getLogger('#status')


class STOMPHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET, broker=None, destination=None):
        super().__init__(level)
        self.destination = destination
        self.broker = broker

    def emit(self, record):
        # if no broker is set, just be silent
        if self.broker is not None and self.destination is not None:
            body = str(record.msg)
            headers = {'content-type': getattr(record.msg, 'content_type', 'text/plain')}
            self.broker.send(self.destination, headers=headers, body=body)


class JSONLogMessage:
    def __init__(self, data):
        self.data = data
        self.content_type = 'application/json'

    def __str__(self):
        return json.dumps(self.data)
