import json
import logging
import os

from argparse import Namespace
from datetime import datetime
from tempfile import NamedTemporaryFile
from time import sleep

from plastron import pcdm
from plastron.stomp import Message
from plastron.exceptions import FailureException, DataReadException, RESTAPIException
from plastron.namespaces import get_manager
from plastron.serializers import SERIALIZER_CLASSES
from plastron.util import LocalFile

logger = logging.getLogger(__name__)
nsm = get_manager()


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='export',
        description='Export resources from the repository'
    )
    parser.add_argument(
        '-o', '--output-file',
        help='File to write export package to',
        action='store',
    )
    parser.add_argument(
        '-f', '--format',
        help='Export job format',
        action='store',
        choices=SERIALIZER_CLASSES.keys(),
        required=True
    )
    parser.add_argument(
        '--uri-template',
        help='Public URI template',
        action='store'
    )
    parser.add_argument(
        'uris',
        nargs='*',
        help='URIs of repository objects to export'
    )
    parser.set_defaults(cmd_name='export')


class Command:
    def __init__(self):
        self.result = None

    def __call__(self, *args, **kwargs):
        for result in self.execute(*args, **kwargs):
            pass

    def execute(self, fcrepo, args):
        start_time = datetime.now().timestamp()
        count = 0
        errors = 0
        total = len(args.uris)
        try:
            serializer_class = SERIALIZER_CLASSES[args.format]
        except KeyError:
            logger.error(f'Unknown format: {args.format}')
            raise FailureException()

        logger.debug(f'Exporting to file {args.output_file}')
        with serializer_class(args.output_file, public_uri_template=args.uri_template) as serializer:
            for uri in args.uris:
                r = fcrepo.head(uri)
                if r.status_code == 200:
                    # do export
                    if 'describedby' in r.links:
                        # the resource is a binary, get the RDF description URI
                        rdf_uri = r.links['describedby']['url']
                    else:
                        rdf_uri = uri
                    logger.info(f'Exporting item {count + 1}/{total}: {uri}')
                    graph = fcrepo.get_graph(rdf_uri)
                    try:
                        serializer.write(graph)
                        count += 1
                    except DataReadException as e:
                        # log the failure, but continue to attempt to export the rest of the URIs
                        logger.error(f'Export of {uri} failed: {e}')
                        errors += 1
                    sleep(1)
                else:
                    # log the failure, but continue to attempt to export the rest of the URIs
                    logger.error(f'Unable to retrieve {uri}')
                    errors += 1

                # update the status
                now = datetime.now().timestamp()
                yield {
                    'time': {
                        'started': start_time,
                        'now': now,
                        'elapsed': now - start_time
                    },
                    'count': {
                        'total': total,
                        'exported': count,
                        'errors': errors
                    }
                }

        logger.info(f'Exported {count} of {total} items')
        self.result = {
            'content_type': serializer.content_type,
            'file_extension': serializer.file_extension,
            'count': {
                'total': total,
                'exported': count,
                'errors': errors
            }
        }


def process_message(listener, message):

    # define the processor for this message
    def process():
        if message.job_id is None:
            logger.error('Expecting a PlastronJobId header')
        else:
            uris = message.body.split('\n')
            export_format = message.args.get('format', 'text/turtle')
            logger.info(f'Received message to initiate export job {message.job_id} containing {len(uris)} items')
            logger.info(f'Requested export format is {export_format}')

            try:
                command = Command()
                with NamedTemporaryFile() as export_fh:
                    logger.debug(f'Export temporary file name is {export_fh.name}')
                    args = Namespace(
                        uris=uris,
                        output_file=export_fh.name,
                        format=export_format,
                        uri_template=listener.public_uri_template
                    )

                    for status in command.execute(listener.repository, args):
                        listener.broker.connection.send(
                            '/topic/plastron.jobs.status',
                            headers={
                                'PlastronJobId': message.job_id
                            },
                            body=json.dumps(status)
                        )

                    job_name = message.args.get('name', message.job_id)
                    filename = job_name + command.result['file_extension']

                    file = pcdm.File(LocalFile(
                        export_fh.name,
                        mimetype=command.result['content_type'],
                        filename=filename
                    ))
                    with listener.repository.at_path('/exports'):
                        file.create_object(repository=listener.repository)
                        command.result['download_uri'] = file.uri
                        logger.info(f'Uploaded export file to {file.uri}')

                    logger.debug(f'Export temporary file size is {os.path.getsize(export_fh.name)}')
                logger.info(f'Export job {message.job_id} complete')
                return Message(
                    headers={
                        'PlastronJobId': message.job_id,
                        'PlastronJobStatus': 'Done',
                        'persistent': 'true'
                    },
                    body=json.dumps(command.result)
                )

            except (FailureException, RESTAPIException) as e:
                logger.error(f"Export job {message.job_id} failed: {e}")
                return Message(
                    headers={
                        'PlastronJobId': message.job_id,
                        'PlastronJobStatus': 'Failed',
                        'PlastronJobError': str(e),
                        'persistent': 'true'
                    }
                )

    # process message
    listener.executor.submit(process).add_done_callback(listener.get_response_handler(message.id))
