#!/usr/bin/env python3
import argparse
import logging
import logging.config
import os
import signal
import sys
import tempfile
import yaml
from datetime import datetime
from stomp import ConnectionListener, Connection
from stomp.exception import ConnectFailedException
from threading import Thread
from plastron import pcdm, version
from plastron.exceptions import ConfigException, RESTAPIException
from plastron.http import Repository
from plastron.commands import export
from plastron.logging import DEFAULT_LOGGING_OPTIONS
from plastron.util import LocalFile

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')


class Exporter:
    def __init__(self, repository, broker, config):
        self.broker = broker
        self.completed_queue = f"/queue/{config['EXPORT_JOBS_COMPLETED_QUEUE']}"
        self.repository = repository
        logger.info(f"Completed export job notifications will go to: {self.completed_queue}")

    def __call__(self, headers, body):
        logger.debug(f'Starting exporter thread')
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

                self.broker.send(self.completed_queue, '', headers={
                    'ArchelonExportJobId': job_id,
                    'ArchelonExportJobStatus': 'Ready',
                    'ArchelonExportJobDownloadUrl': file.uri,
                    'persistent': 'true'
                })
                logger.debug(f'Completed exporter thread for job id {job_id}')
                logger.info(f'Export job {job_id} complete')

            except (ConfigException, RESTAPIException) as e:
                self.broker.send(self.completed_queue, '', headers={
                    'ArchelonExportJobId': job_id,
                    'ArchelonExportJobStatus': 'Failed',
                    'ArchelonExportJobError': str(e),
                    'persistent': 'true'
                })
                logger.error(f"Export job {job_id} failed: {e}")


class Listener(ConnectionListener):
    def __init__(self, destination_map):
        self.destination_map = destination_map

    def on_message(self, headers, body):
        logger.debug(f'Received message with headers: {headers}')
        destination = headers['destination']
        if destination in self.destination_map:
            handler = self.destination_map[destination]
            # spawn a new thread to handle this message
            logger.debug(f'Creating thread to handle message on {destination}')
            Thread(target=handler, kwargs={'headers': headers, 'body': body}).start()
        else:
            logger.debug(f'No destination handler for {destination}')


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

    # setup handlers for messages on specific queues
    exporter = Exporter(repo, broker, exporter_config)
    destination_map = {
        f"/queue/{exporter_config['EXPORT_JOBS_QUEUE']}": exporter
    }

    broker.set_listener('', Listener(destination_map))
    broker.start()

    try:
        broker.connect()
    except ConnectFailedException:
        logger.error(f"Connection to STOMP message broker at {broker_config['SERVER']} failed")
        sys.exit(1)

    logger.info(f"Connected to STOMP message broker: {broker_config['SERVER']}")

    for queue in destination_map.keys():
        broker.subscribe(destination=queue, id='plastron')
        logger.info(f"Subscribed to {queue}")

    try:
        while True:
            signal.pause()
    except KeyboardInterrupt:
        sys.exit()


if __name__ == "__main__":
    main()
