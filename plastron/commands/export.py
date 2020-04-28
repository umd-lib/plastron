import logging
from argparse import Namespace, FileType
from datetime import datetime
from plastron import pcdm
from plastron.exceptions import FailureException, DataReadException
from plastron.namespaces import get_manager
from plastron.serializers import SERIALIZER_CLASSES
from plastron.files import LocalFile
from tempfile import NamedTemporaryFile
from time import sleep

logger = logging.getLogger(__name__)
nsm = get_manager()


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='export',
        description='Export resources from the repository'
    )
    file_or_upload = parser.add_mutually_exclusive_group()
    file_or_upload.add_argument(
        '-o', '--output-file',
        help='File to write export package to',
        type=FileType('w'),
        action='store',
    )
    file_or_upload.add_argument(
        '--upload-to',
        help='Repository path to POST the export file to',
        dest='upload_path',
        action='store'
    )
    parser.add_argument(
        '--upload-name',
        help='Used to create the download filename for the uploaded export file in the repository',
        dest='upload_filename',
        action='store'
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


def parse_message(message):
    uris = message.body.split('\n')
    export_format = message.args.get('format', 'text/turtle')
    logger.info(f'Received message to initiate export job {message.job_id} containing {len(uris)} items')
    logger.info(f'Requested export format is {export_format}')

    return Namespace(
        uris=uris,
        output_file=None,
        upload_path='/exports',
        upload_filename=message.args.get('name', message.job_id),
        format=export_format,
        uri_template=message.args.get('uri-template')
    )


class Command:
    def __init__(self):
        self.result = None

    def __call__(self, *args, **kwargs):
        for _ in self.execute(*args, **kwargs):
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

        if args.output_file is None:
            args.output_file = NamedTemporaryFile(mode='w+')

        logger.debug(f'Exporting to file {args.output_file.name}')
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

        download_uri = None
        # upload to the repo if requested
        if args.upload_path is not None:
            if count == 0:
                logger.warning('No items exported, skipping upload to repository')
            else:
                if args.upload_filename is None:
                    args.upload_filename = 'export_' + datetime.utcnow().strftime('%Y%m%d%H%M%S')
                filename = args.upload_filename + serializer.file_extension
                # rewind to the beginning of the file
                args.output_file.seek(0)

                file = pcdm.File(LocalFile(
                    args.output_file.name,
                    mimetype=serializer.content_type,
                    filename=filename
                ))
                with fcrepo.at_path(args.upload_path):
                    file.create_object(repository=fcrepo)
                    download_uri = file.uri
                    logger.info(f'Uploaded export file to {file.uri}')

        self.result = {
            'content_type': serializer.content_type,
            'file_extension': serializer.file_extension,
            'download_uri': download_uri,
            'count': {
                'total': total,
                'exported': count,
                'errors': errors
            }
        }
