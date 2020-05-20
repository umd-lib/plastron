import logging
import os
from argparse import Namespace, FileType
from datetime import datetime
from distutils.util import strtobool
from email.utils import parsedate
from paramiko import SFTPClient
from plastron import pcdm
from plastron.exceptions import FailureException, DataReadException, RESTAPIException
from plastron.files import LocalFile
from plastron.namespaces import get_manager
from plastron.pcdm import Object
from plastron.serializers import SERIALIZER_CLASSES
from plastron.util import get_ssh_client
from tempfile import NamedTemporaryFile
from urllib.parse import urlsplit
from zipfile import ZipFile, ZipInfo


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
        '--export-binaries',
        help='Export binaries in addition to the metadata. Requires --binaries-file to be present',
        action='store_true'
    )
    parser.add_argument(
        '--binaries-file',
        help='File to write exported binaries to',
        type=FileType('wb'),
        action='store'
    )
    parser.add_argument(
        'uris',
        nargs='*',
        help='URIs of repository objects to export'
    )
    parser.set_defaults(cmd_name='export')


def format_size(size):
    if size < 1024:
        return size, 'B'
    size /= 1024
    if size < 1024:
        return size, 'KB'
    size /= 1024
    if size < 1024:
        return size, 'MB'
    size /= 1024
    if size < 2014:
        return size, 'GB'
    size /= 2014
    return size, 'TB'


class Command:
    def __init__(self, config):
        self.binaries_dest = config.get('BINARIES_DEST', os.path.curdir)
        self.exports_collection = config.get('COLLECTION', '/exports')
        self.result = None

    def __call__(self, *args, **kwargs):
        for _ in self.execute(*args, **kwargs):
            pass

    def parse_message(self, message):
        uris = message.body.split('\n')
        export_format = message.args.get('format', 'text/turtle')
        logger.info(f'Received message to initiate export job {message.job_id} containing {len(uris)} items')
        logger.info(f'Requested export format is {export_format}')
        upload_filename = message.args.get('name', message.job_id)
        export_binaries = bool(strtobool(message.args.get('export-binaries', 'false')))
        if export_binaries:
            binaries_filename = upload_filename + '_binaries.zip'
            logger.info(f'Binaries will be saved to {os.path.join(self.binaries_dest, binaries_filename)}')
            if self.binaries_dest.startswith('sftp:'):
                # remote (SFTP) destination
                sftp_uri = urlsplit(self.binaries_dest)
                ssh_client = get_ssh_client(sftp_uri)
                sftp_client = SFTPClient.from_transport(ssh_client.get_transport())
                binaries_file = sftp_client.open(os.path.join(sftp_uri.path, binaries_filename), mode='wb')
            else:
                # assume a local directory
                binaries_filename = os.path.join(self.binaries_dest, binaries_filename)
                binaries_file = open(binaries_filename, mode='wb')
        else:
            binaries_file = None

        return Namespace(
            uris=uris,
            output_file=None,
            upload_path=self.exports_collection,
            upload_filename=upload_filename,
            format=export_format,
            uri_template=message.args.get('uri-template'),
            export_binaries=export_binaries,
            binaries_file=binaries_file
        )

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

        if args.export_binaries:
            if args.binaries_file is None:
                raise FailureException('Option --export-binaries requires --binaries-file [filename]')
            binaries_zip = ZipFile(args.binaries_file, mode='w')
        else:
            binaries_zip = None

        logger.debug(f'Exporting to file {args.output_file.name}')
        serializer = serializer_class(args.output_file, public_uri_template=args.uri_template)
        for uri in args.uris:
            try:
                logger.info(f'Exporting item {count + 1}/{total}: {uri}')
                obj = Object.from_repository(fcrepo, uri=uri)
                if args.export_binaries:
                    logger.info(f'Gathering binaries for {uri}')
                    binaries = list(obj.gather_files(fcrepo))
                    total_size = sum(int(file.size[0]) for file in binaries)
                    size, unit = format_size(total_size)
                    logger.info(f'Total size of binaries: {round(size, 2)} {unit}')
                else:
                    binaries = None

                serializer.write(obj.graph(), files=binaries)

                if binaries is not None:
                    for file in binaries:
                        response = fcrepo.head(file.uri)
                        modified = parsedate(response.headers['Last-Modified'])
                        info = ZipInfo(filename=str(file.filename), date_time=modified[:6])
                        logger.info(f'Adding {info.filename} to zip file')
                        with binaries_zip.open(info, mode='w') as binary:
                            for chunk in file.source.data():
                                binary.write(chunk)
                count += 1

            except DataReadException as e:
                # log the failure, but continue to attempt to export the rest of the URIs
                logger.error(f'Export of {uri} failed: {e}')
                errors += 1
            except RESTAPIException as e:
                # log the failure, but continue to attempt to export the rest of the URIs
                logger.error(f'Unable to retrieve {uri}: {e}')
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

        serializer.finish()

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
