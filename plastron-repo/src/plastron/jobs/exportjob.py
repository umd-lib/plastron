import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate
from os.path import normpath, relpath, splitext, basename
from tempfile import TemporaryDirectory
from time import mktime
from typing import Optional, List
from urllib.parse import urlsplit
from zipfile import ZipFile

from bagit import make_bag
from paramiko import SFTPClient, SSHException
from requests import ConnectionError

from plastron.client import Client, ClientError
from plastron.files import get_ssh_client
from plastron.models import Item
from plastron.rdf.pcdm import File
from plastron.repo import DataReadError, Repository, RepositoryResource
from plastron.serializers import SERIALIZER_CLASSES, detect_resource_class, EmptyItemListError

UUID_REGEX = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)

logger = logging.getLogger(__name__)


def format_size(size: int, decimal_places: Optional[int] = None):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if size < 1024:
            break
        size /= 1024

    if decimal_places is not None:
        return round(size, decimal_places), unit


def compress_bag(bag, dest, root_dirname=''):
    with ZipFile(dest, mode='w') as zip_file:
        for dirpath, dirnames, filenames in os.walk(bag.path):
            for name in filenames:
                src_filename = os.path.join(dirpath, name)
                archived_name = normpath(os.path.join(root_dirname, relpath(dirpath, start=bag.path), name))
                zip_file.write(filename=src_filename, arcname=archived_name)


@dataclass
class ExportJob:
    repo: Repository
    export_format: str
    export_binaries: bool
    binary_types: str
    output_dest: str
    uri_template: str
    uris: List[str]
    key: str

    def list_binaries_to_export(self, obj) -> Optional[List[File]]:
        if self.export_binaries and self.binary_types is not None:
            # filter files by their MIME type
            def mime_type_filter(file):
                return str(file.mimetype) in self.binary_types.split(',')
        else:
            # default filter is None; in this case filter() will return
            # all items that evaluate to true
            mime_type_filter = None

        if self.export_binaries:
            logger.info(f'Gathering binaries for {obj.uri}')
            binaries = list(filter(mime_type_filter, obj.gather_files(self.client)))
            total_size = sum(int(file.size[0]) for file in binaries)
            size, unit = format_size(total_size, decimal_places=2)
            logger.info(f'Total size of binaries: {size} {unit}')
        else:
            binaries = None

        return binaries

    def run(self):
        logger.info(f'Requested export format is {self.export_format}')

        start_time = datetime.now().timestamp()
        count = 0
        errors = 0
        total = len(self.uris)
        try:
            serializer_class = SERIALIZER_CLASSES[self.export_format]
        except KeyError:
            raise RuntimeError(f'Unknown format: {self.export_format}')

        logger.info(f'Export destination: {self.output_dest}')

        # create a bag in a temporary directory to hold exported items
        temp_dir = TemporaryDirectory()
        bag = make_bag(temp_dir.name)

        export_dir = os.path.join(temp_dir.name, 'data')
        serializer = serializer_class(directory=export_dir, public_uri_template=self.uri_template)
        for uri in self.uris:
            try:
                logger.info(f'Exporting item {count + 1}/{total}: {uri}')

                # derive an item-level directory name from the URI
                # currently this is hard-coded to look for a UUID
                # TODO: expand to other types of unique ids?
                match = UUID_REGEX.search(uri)
                if match is None:
                    raise DataReadError(f'No UUID found in {uri}')
                item_dir = match[0]

                resource: RepositoryResource = self.repo.get_resource(path=uri)
                resource.read()

                model_class = detect_resource_class(resource.graph, resource.url, fallback=Item)
                obj = resource.describe(model=model_class)
                binaries = self.list_binaries_to_export(obj)

                # write the metadata for this object
                serializer.write(obj, files=binaries, binaries_dir=item_dir)

                if binaries is not None:
                    binaries_dir = os.path.join(export_dir, item_dir)
                    os.makedirs(binaries_dir, exist_ok=True)
                    for file in binaries:
                        response = self.client.head(file.uri)
                        accessed = parsedate(response.headers['Date'])
                        modified = parsedate(response.headers['Last-Modified'])

                        binary_filename = os.path.join(binaries_dir, str(file.filename))
                        with open(binary_filename, mode='wb') as binary:
                            with file.source as stream:
                                for chunk in stream:
                                    binary.write(chunk)

                        # update the atime and mtime of the file to reflect the time of the
                        # HTTP request and the resource's last-modified time in the repo
                        os.utime(binary_filename, times=(mktime(accessed), mktime(modified)))
                        logger.debug(f'Copied {file.uri} to {binary.name}')

                count += 1

            except DataReadError as e:
                # log the failure, but continue to attempt to export the rest of the URIs
                logger.error(f'Export of {uri} failed: {e}')
                errors += 1
            except (ClientError, ConnectionError) as e:
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
        if self.output_dest.startswith('sftp:'):
            # send over SFTP to a remote host
            sftp_uri = urlsplit(self.output_dest)
            ssh_client = get_ssh_client(sftp_uri, key_filename=self.key)
            try:
                sftp_client = SFTPClient.from_transport(ssh_client.get_transport())
                root, ext = splitext(basename(sftp_uri.path))
                destination = sftp_client.open(sftp_uri.path, mode='w')
            except SSHException as e:
                raise RuntimeError(str(e)) from e
        else:
            # send to a local file
            zip_filename = self.output_dest
            root, ext = splitext(basename(zip_filename))
            destination = zip_filename

        # write out a single ZIP file of the whole bag
        compress_bag(bag, destination, root)

        return {
            'type': 'export_complete' if count == total else 'partial_export',
            'content_type': serializer.content_type,
            'file_extension': serializer.file_extension,
            'count': {
                'total': total,
                'exported': count,
                'errors': errors
            }
        }
