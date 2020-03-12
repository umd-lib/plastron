from datetime import datetime
import logging
import os

from argparse import Namespace
from tempfile import NamedTemporaryFile
from time import sleep

from plastron import pcdm
from plastron.stomp import Message
from plastron.exceptions import ConfigException, DataReadException, RESTAPIException
from plastron.logging import JSONLogMessage, STATUS_LOGGER
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
    def __call__(self, fcrepo, args):
        start_time = datetime.now().timestamp()
        count = 0
        errors = 0
        total = len(args.uris)
        try:
            serializer_class = SERIALIZER_CLASSES[args.format]
        except KeyError:
            raise ConfigException(f'Unknown format: {args.format}')

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
                STATUS_LOGGER.info(JSONLogMessage({
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
                }))

        logger.info(f'Exported {count} of {total} items')
        return {
            'content_type': serializer.content_type,
            'file_extension': serializer.file_extension,
            'count': {
                'total': total,
                'exported': count,
                'errors': errors
            }
        }


def process_message(listener, message_id, headers, body):

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
                command = Command()
                with NamedTemporaryFile() as export_fh:
                    logger.debug(f'Export temporary file name is {export_fh.name}')
                    args = Namespace(
                        uris=uris,
                        output_file=export_fh.name,
                        format=export_format,
                        uri_template=listener.public_uri_template
                    )
                    result = command(listener.repository, args)

                    job_name = headers.get('ArchelonExportJobName', job_id)
                    filename = job_name + result['file_extension']

                    file = pcdm.File(LocalFile(export_fh.name, mimetype=result['content_type'], filename=filename))
                    with listener.repository.at_path('/exports'):
                        file.create_object(repository=listener.repository)
                        logger.info(f'Uploaded export file to {file.uri}')

                    logger.debug(f'Export temporary file size is {os.path.getsize(export_fh.name)}')
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

    # define the response handler for this message
    def handle_response(future):
        response = future.result()

        # save a copy of the response message in the outbox
        job_id = response.headers['ArchelonExportJobId']
        listener.outbox.add(job_id, response)

        # remove the message from the inbox now that processing has completed
        listener.inbox.remove(message_id)

        # send the job completed message
        listener.broker.send(listener.completed_queue, headers=response.headers, body=response.body)

        # remove the message from the outbox now that sending has completed
        listener.outbox.remove(job_id)

    # process message
    listener.executor.submit(process).add_done_callback(handle_response)
