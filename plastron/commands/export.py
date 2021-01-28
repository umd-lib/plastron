import logging
import os
import re
from argparse import Namespace
from bagit import make_bag
from datetime import datetime
from distutils.util import strtobool
from email.utils import parsedate
from os.path import basename, normpath, relpath, splitext
from paramiko import SFTPClient, SSHException

from plastron.commands import BaseCommand
from plastron.exceptions import FailureException, DataReadException, RESTAPIException
from plastron.namespaces import get_manager
from plastron.pcdm import Object
from plastron.serializers import EmptyItemListError, SERIALIZER_CLASSES, detect_resource_class
from plastron.util import get_ssh_client
from tempfile import TemporaryDirectory
from time import mktime
from urllib.parse import urlsplit
from zipfile import ZipFile


logger = logging.getLogger(__name__)
nsm = get_manager()
UUID_REGEX = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='export',
        description='Export resources from the repository as a BagIt bag'
    )
    parser.add_argument(
        '-o', '--output-dest',
        help='Where to send the export. Can be a local filename or an SFTP URI',
        required=True,
        action='store'
    )
    parser.add_argument(
        '--key',
        help='SSH private key file to use for SFTP connections',
        action='store'
    )
    parser.add_argument(
        '-f', '--format',
        help='Format for exported metadata',
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
        '-B', '--export-binaries',
        help='Export binaries in addition to the metadata',
        action='store_true'
    )
    parser.add_argument(
        '--binary-types',
        help='Include only binaries with a MIME type from this list',
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
    if size < 1024:
        return size, 'GB'
    size /= 2014
    return size, 'TB'


class Command(BaseCommand):
    def __init__(self, config=None):
        super().__init__(config=config)
        self.ssh_private_key = self.config.get('SSH_PRIVATE_KEY')
        self.result = None

    def __call__(self, *args, **kwargs):
        for _ in self.execute(*args, **kwargs):
            pass

    def parse_message(self, message):
        uris = message.body.split('\n')
        export_format = message.args.get('format', 'text/turtle')
        logger.info(f'Received message to initiate export job {message.job_id} containing {len(uris)} items')
        logger.info(f'Requested export format is {export_format}')
        export_binaries = bool(strtobool(message.args.get('export-binaries', 'false')))

        return Namespace(
            uris=uris,
            output_dest=message.args.get('output-dest'),
            format=export_format,
            uri_template=message.args.get('uri-template'),
            export_binaries=export_binaries,
            binary_types=message.args.get('binary-types'),
            key=self.ssh_private_key
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

        if args.export_binaries and args.binary_types is not None:
            # filter files by their MIME type
            def mime_type_filter(file):
                return str(file.mimetype) in args.binary_types.split(',')
        else:
            # default filter is None; in this case filter() will return
            # all items that evaluate to true
            mime_type_filter = None

        logger.info(f'Export destination: {args.output_dest}')

        # create a bag in a temporary directory to hold exported items
        temp_dir = TemporaryDirectory()
        bag = make_bag(temp_dir.name)

        export_dir = os.path.join(temp_dir.name, 'data')
        serializer = serializer_class(directory=export_dir, public_uri_template=args.uri_template)
        for uri in args.uris:
            try:
                logger.info(f'Exporting item {count + 1}/{total}: {uri}')

                # derive an item-level directory name from the URI
                # currently this is hard-coded to look for a UUID
                # TODO: expand to other types of unique ids?
                match = UUID_REGEX.search(uri)
                if match is None:
                    raise DataReadException(f'No UUID found in {uri}')
                item_dir = match[0]

                graph = fcrepo.get_graph(uri)
                model_class = detect_resource_class(graph, uri, fallback=Object)
                obj = model_class.from_graph(graph, uri)

                if args.export_binaries:
                    logger.info(f'Gathering binaries for {uri}')
                    binaries = list(filter(mime_type_filter, obj.gather_files(fcrepo)))
                    total_size = sum(int(file.size[0]) for file in binaries)
                    size, unit = format_size(total_size)
                    logger.info(f'Total size of binaries: {round(size, 2)} {unit}')
                else:
                    binaries = None

                serializer.write(obj.graph(), files=binaries, binaries_dir=item_dir)

                if binaries is not None:
                    binaries_dir = os.path.join(export_dir, item_dir)
                    os.makedirs(binaries_dir, exist_ok=True)
                    for file in binaries:
                        response = fcrepo.head(file.uri)
                        accessed = parsedate(response.headers['Date'])
                        modified = parsedate(response.headers['Last-Modified'])

                        binary_filename = os.path.join(binaries_dir, str(file.filename))
                        with open(binary_filename, mode='wb') as binary:
                            with file.source as stream:
                                for chunk in stream:
                                    binary.write(chunk)

                        # update the atime a mtime of the file to reflect the time of the
                        # HTTP request and the resource's last-modified time in the repo
                        os.utime(binary_filename, times=(mktime(accessed), mktime(modified)))
                        logger.debug(f'Copied {file.uri} to {binary.name}')

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

        try:
            serializer.finish()
        except EmptyItemListError:
            logger.error("No items could be exported; skipping writing file")

        logger.info(f'Exported {count} of {total} items')

        # save the BagIt bag to send to the output destination
        bag.save(manifests=True)

        # parse the output destination to determine where to send the export
        if args.output_dest.startswith('sftp:'):
            # send over SFTP to a remote host
            sftp_uri = urlsplit(args.output_dest)
            ssh_client = get_ssh_client(sftp_uri, key_filename=args.key)
            try:
                sftp_client = SFTPClient.from_transport(ssh_client.get_transport())
                root, ext = splitext(basename(sftp_uri.path))
                destination = sftp_client.open(sftp_uri.path, mode='w')
            except SSHException as e:
                raise FailureException(str(e)) from e
        else:
            # send to a local file
            zip_filename = args.output_dest
            root, ext = splitext(basename(zip_filename))
            destination = zip_filename

        # write out a single ZIP file of the whole bag
        compress_bag(bag, destination, root)

        self.result = {
            'content_type': serializer.content_type,
            'file_extension': serializer.file_extension,
            'count': {
                'total': total,
                'exported': count,
                'errors': errors
            }
        }


def compress_bag(bag, dest, root_dirname=''):
    with ZipFile(dest, mode='w') as zip_file:
        for dirpath, dirnames, filenames in os.walk(bag.path):
            for name in filenames:
                src_filename = os.path.join(dirpath, name)
                archived_name = normpath(os.path.join(root_dirname, relpath(dirpath, start=bag.path), name))
                zip_file.write(filename=src_filename, arcname=archived_name)
