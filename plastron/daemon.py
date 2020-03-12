#!/usr/bin/env python3
import argparse
import logging
import logging.config
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from importlib import import_module
from time import sleep

import yaml
from stomp import Connection, ConnectionListener
from stomp.exception import ConnectFailedException

from plastron import version
from plastron.http import Repository
from plastron.logging import DEFAULT_LOGGING_OPTIONS, STOMPHandler, STATUS_LOGGER

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')


class Message:
    def read(self, filename):
        self.headers = {}
        self.body = ''
        in_body = False
        with open(filename, 'r') as fh:
            for line in fh:
                if in_body:
                    self.body += line
                else:
                    if line.rstrip() == '':
                        in_body = True
                        continue
                    else:
                        (key, value) = line.split(':', 1)
                        self.headers[key] = value.strip()
        return self

    def __init__(self, headers=None, body=''):
        if headers is not None:
            self.headers = headers
        else:
            self.headers = {}
        self.body = body

    def __str__(self):
        return '\n'.join([f'{k}: {v}' for k, v in self.headers.items()]) + '\n\n' + self.body


class MessageBox:
    def __init__(self, directory):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)

    def add(self, id, message):
        filename = os.path.join(self.dir, id)
        with open(filename, 'wb') as fh:
            fh.write(str(message).encode())

    def remove(self, id):
        filename = os.path.join(self.dir, id)
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
            return Message().read(filename)
        else:
            raise StopIteration


class CommandListener(ConnectionListener):
    def __init__(self, broker, repository):
        self.broker = broker
        self.repository = repository
        self.queue = self.broker.destinations['JOBS_QUEUE']
        self.completed_queue = self.broker.destinations['COMPLETED_JOBS_QUEUE']
        self.inbox = MessageBox(os.path.join(self.broker.message_store_dir, 'inbox'))
        self.outbox = MessageBox(os.path.join(self.broker.message_store_dir, 'outbox'))
        self.executor = ThreadPoolExecutor(thread_name_prefix='CommandListener')
        self.public_uri_template = self.broker.public_uri_template

    def on_connected(self, headers, body):
        # first attempt to send anything in the outbox
        for message in self.outbox:
            job_id = message.headers['ArchelonExportJobId']
            logger.info(f"Found response message for job {job_id} in outbox")
            # send the job completed message
            self.broker.connection.send(self.completed_queue, headers=message.headers, body=message.body)
            logger.info(f'Sent response message for job {job_id}')
            # remove the message from the outbox now that sending has completed
            self.outbox.remove(job_id)

        # then process anything in the inbox
        for message in self.inbox:
            self.dispatch(message)

        # then subscribe to the queue to receive incoming messages
        self.broker.connection.subscribe(destination=self.queue, id='plastron')
        logger.info(f"Subscribed to {self.queue}")

    def dispatch(self, message):
        # determine which command to load to process it
        command_name = message.headers['PlastronCommand']
        command_module = import_module('plastron.commands.' + command_name)
        # TODO: cache the command modules
        # TODO: check that process_message exists in the command module
        command_module.process_message(self, message.headers['message-id'], message.headers, message.body)

    def on_message(self, headers, body):
        if headers['destination'] == self.queue:
            logger.debug(f'Received message on {self.queue} with headers: {headers}')

            # save the message in the inbox until we can process it
            message_id = headers['message-id']
            message = Message(headers=headers, body=body)
            self.inbox.add(message_id, message)

            # and then process the message
            self.dispatch(message)


class ReconnectListener(ConnectionListener):
    def __init__(self, broker):
        self.broker = broker

    def on_connecting(self, host_and_port):
        logger.info(f'Connecting to STOMP message broker at {":".join(host_and_port)}')

    def on_connected(self, headers, body):
        logger.info('Connected to STOMP message broker')

    def on_disconnected(self):
        logger.warning('Disconnected from the STOMP message broker')
        self.broker.connect()




class Broker:
    def __init__(self, config):
        # set up STOMP client
        broker_server = tuple(config['SERVER'].split(':', 2))
        self.connection = Connection([broker_server])
        self.destinations = config['DESTINATIONS']
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


def main():
    parser = argparse.ArgumentParser(
        prog='plastron',
        description='Batch operations daemon for Fedora 4.'
    )
    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file.',
        action='store',
        required=True
    )
    parser.add_argument(
        '-v', '--verbose',
        help='increase the verbosity of the status output',
        action='store_true'
    )

    # parse command line args
    args = parser.parse_args()

    with open(args.config, 'r') as config_file:
        config = yaml.safe_load(config_file)

    repo_config = config['REPOSITORY']
    broker_config = config['MESSAGE_BROKER']

    logging_options = DEFAULT_LOGGING_OPTIONS

    # log file configuration
    log_dirname = repo_config.get('LOG_DIR')
    if not os.path.isdir(log_dirname):
        os.makedirs(log_dirname)
    log_filename = f'plastron.daemon.{now}.log'
    logfile = os.path.join(log_dirname, log_filename)
    logging_options['handlers']['file']['filename'] = logfile

    # manipulate console verbosity
    if args.verbose:
        logging_options['handlers']['console']['level'] = 'DEBUG'

    # configure logging
    logging.config.dictConfig(logging_options)

    # configure STOMP message broker
    broker = Broker(broker_config)

    # set up status logging to STOMP
    stomp_handler = STOMPHandler(connection=broker.connection, destination=broker.destinations['JOB_STATUS'])
    STATUS_LOGGER.addHandler(stomp_handler)

    logger.info(f'plastrond {version}')

    repo = Repository(repo_config, ua_string=f'plastron/{version}')

    # setup listeners
    broker.connection.set_listener('reconnect', ReconnectListener(broker))
    broker.connection.set_listener('export', CommandListener(broker, repo))

    try:
        broker.connect()
        while True:
            signal.pause()
    except KeyboardInterrupt:
        sys.exit()


if __name__ == "__main__":
    main()
