#!/usr/bin/env python3
import argparse
import logging
import logging.config
import os
import signal
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import yaml
from datetime import datetime
from stomp import ConnectionListener, Connection
from stomp.exception import ConnectFailedException
from plastron import pcdm, version
from plastron.exceptions import ConfigException, RESTAPIException
from plastron.http import Repository
from plastron.commands import export
from plastron.logging import DEFAULT_LOGGING_OPTIONS
from plastron.util import LocalFile

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


class ExportListener(ConnectionListener):
    def __init__(self, broker, repository, config):
        self.broker = broker
        self.repository = repository
        self.queue = '/queue/' + config['EXPORT_JOBS_QUEUE']
        self.completed_queue = '/queue/' + config['EXPORT_JOBS_COMPLETED_QUEUE']
        self.inbox = MessageBox(os.path.join(config['MESSAGE_STORE_DIR'], 'inbox'))
        self.outbox = MessageBox(os.path.join(config['MESSAGE_STORE_DIR'], 'outbox'))
        self.executor = ThreadPoolExecutor(thread_name_prefix='ExportListener')

    def on_connecting(self, host_and_port):
        logger.info(f'Connecting to STOMP message broker at {":".join(host_and_port)}')

    def on_connected(self, headers, body):
        logger.info('Connected to STOMP message broker')
        # first attempt to send anything in the outbox
        for message in self.outbox:
            job_id = message.headers['ArchelonExportJobId']
            logger.info(f"Found response message for job id {job_id} in outbox")
            # send the job completed message
            self.broker.send(self.completed_queue, headers=message.headers, body=message.body)
            logger.info(f'Sent response message for job id {job_id}')
            # remove the message from the outbox now that sending has completed
            self.outbox.remove(job_id)

        # then process anything in the inbox
        for message in self.inbox:
            message_id = message.headers['message-id']
            self.process_message(message_id, message.headers, message.body)

        self.broker.subscribe(destination=self.queue, id='plastron')
        logger.info(f"Subscribed to {self.queue}")

    def on_disconnected(self):
        logger.warning('Disconnected from the STOMP server')
        while not self.broker.is_connected():
            logger.info('Attempting to reconnect')
            try:
                self.broker.connect(wait=True)
            except ConnectFailedException:
                logger.warning('Reconnection attempt failed')
                sleep(1)


    def on_message(self, headers, body):
        if headers['destination'] == self.queue:
            logger.debug(f'Received message on {self.queue} with headers: {headers}')

            # save the message in the inbox until we can process it
            message_id = headers['message-id']
            message = Message(headers=headers, body=body)
            self.inbox.add(message_id, message)

            self.process_message(message_id, headers, body)

    def process_message(self, message_id, headers, body):

        # define the response handler for this message
        def handle_response(future):
            response = future.result()

            # save a copy of the response message in the outbox
            job_id = response.headers['ArchelonExportJobId']
            self.outbox.add(job_id, response)

            # remove the message from the inbox now that processing has completed
            self.inbox.remove(message_id)

            # send the job completed message
            self.broker.send(self.completed_queue, headers=response.headers, body=response.body)

            # remove the message from the outbox now that sending has completed
            self.outbox.remove(job_id)

        # define the processor for this message
        def process():
            job_id = headers.get('ArchelonExportJobId', None)
            if job_id is None:
                logger.error('Expecting an ArchelonExportJobId header')
            else:
                uris = body.split('\n')
                export_format = headers.get('ArchelonExportJobFormat', 'text/turtle')
                logger.info(f'Received message to initiate export job with id {job_id} containing {len(uris)} items')
                logger.info(f'Requested export format is {export_format}')

                try:
                    command = export.Command()
                    with tempfile.NamedTemporaryFile() as export_fh:
                        args = argparse.Namespace(uris=uris, output_file=export_fh.name, format=export_format)
                        command(self.repository, args)

                        file = pcdm.File(source=LocalFile(export_fh.name, mimetype=export_format))
                        with self.repository.at_path('/exports'):
                            file.create_object(repository=self.repository)

                    logger.info(f'Export job {job_id} complete')
                    return Message(
                        headers={
                            'ArchelonExportJobId': job_id,
                            'ArchelonExportJobStatus': 'Ready',
                            'ArchelonExportJobDownloadUrl': file.uri,
                            'persistent': 'true'
                        }
                    )

                except (ConfigException, RESTAPIException) as e:
                    logger.error(f"Export job {job_id} failed: {e}")
                    return Message(
                        headers={
                            'ArchelonExportJobId': job_id,
                            'ArchelonExportJobStatus': 'Failed',
                            'ArchelonExportJobError': str(e),
                            'persistent': 'true'
                        }
                    )

        # process message
        self.executor.submit(process).add_done_callback(handle_response)

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
    exporter_config = config['EXPORTER']

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

    logger.info(f'plastrond {version}')

    repo = Repository(repo_config, ua_string=f'plastron/{version}')

    broker_server = tuple(broker_config['SERVER'].split(':', 2))
    broker = Connection([broker_server])

    # setup listeners
    broker.set_listener('export', ExportListener(broker, repo, exporter_config))

    try:
        broker.connect()
    except ConnectFailedException:
        logger.error(f"Connection to STOMP message broker at {broker_config['SERVER']} failed")
        sys.exit(1)

    try:
        while True:
            signal.pause()
    except KeyboardInterrupt:
        sys.exit()


if __name__ == "__main__":
    main()
