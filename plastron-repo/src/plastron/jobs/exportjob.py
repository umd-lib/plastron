import logging
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate
from os.path import normpath, relpath, splitext, basename
from tempfile import TemporaryDirectory
from time import mktime
from typing import Optional, List, Generator, Dict, Any, Iterator
from urllib.parse import urlsplit
from zipfile import ZipFile

from bagit import make_bag
from paramiko import SFTPClient, SSHException
from requests import ConnectionError

from plastron.client import ClientError
from plastron.files import get_ssh_client
from plastron.models import Item
from plastron.models.umd import PCDMFile
from plastron.repo import DataReadError, Repository, BinaryResource
from plastron.repo.pcdm import PCDMObjectResource, AggregationResource, PCDMFileBearingResource
from plastron.serializers import SERIALIZER_CLASSES, detect_resource_class
from plastron.serializers.csv import EmptyItemListError

UUID_REGEX = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)

logger = logging.getLogger(__name__)


def format_size(size: int, decimal_places: Optional[int] = None):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if size < 1024:
            break
        size /= 1024

    if decimal_places is not None:
        return round(size, decimal_places), unit
    else:
        return size, unit


def compress_bag(bag, dest, root_dirname=''):
    with ZipFile(dest, mode='w') as zip_file:
        for dirpath, dirnames, filenames in os.walk(bag.path):
            for name in filenames:
                src_filename = os.path.join(dirpath, name)
                archived_name = normpath(os.path.join(root_dirname, relpath(dirpath, start=bag.path), name))
                zip_file.write(filename=src_filename, arcname=archived_name)


def gather_files(resource: AggregationResource, mime_type: str = None) -> Iterator[BinaryResource]:
    for page_url in resource.get_sequence():
        page_resource = resource.repo[page_url:PCDMFileBearingResource]
        for file in page_resource.get_files(mime_type=mime_type):
            yield file


class Stopwatch:
    def __init__(self):
        self._start = datetime.now().timestamp()

    def now(self) -> Dict[str, float]:
        now = datetime.now().timestamp()
        return {
            'started': self._start,
            'now': now,
            'elapsed': now - self._start
        }


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

    def list_binaries_to_export(self, resource: PCDMObjectResource) -> Optional[List[BinaryResource]]:
        if self.export_binaries and self.binary_types is not None:
            accepted_types = self.binary_types.split(',')

            # filter files by their MIME type
            def mime_type_filter(file):
                return str(file.headers['Content-Type']) in accepted_types
        else:
            # default filter is None; in this case filter() will return
            # all items that evaluate to true
            mime_type_filter = None

        if self.export_binaries:
            logger.info(f'Gathering binaries for {resource.url}')
            binaries = list(filter(mime_type_filter, gather_files(resource)))
            total_size = sum(file_resource.size for file_resource in binaries)
            size, unit = format_size(total_size, decimal_places=2)
            logger.info(f'Total size of binaries: {size} {unit}')
        else:
            binaries = None

        return binaries

    def run(self) -> Generator[Dict[str, Any], None, Dict[str, Any]]:
        logger.info(f'Requested export format is {self.export_format}')

        timer = Stopwatch()
        count = Counter(
            total=len(self.uris),
            exported=0,
            errors=0,
        )

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
                logger.info(f'Exporting item {count["exported"] + 1}/{count["total"]}: {uri}')

                # derive an item-level directory name from the URI
                # currently this is hard-coded to look for a UUID
                # TODO: expand to other types of unique ids?
                match = UUID_REGEX.search(uri)
                if match is None:
                    raise DataReadError(f'No UUID found in {uri}')
                item_dir = match[0]

                resource = self.repo[uri:PCDMObjectResource].read()

                model_class = detect_resource_class(resource.graph, resource.url, fallback=Item)
                binaries = self.list_binaries_to_export(resource)

                # write the metadata for this object
                obj = resource.describe(model=model_class)
                serializer.write(obj, files=binaries, binaries_dir=item_dir)

                if binaries is not None:
                    binaries_dir = os.path.join(export_dir, item_dir)
                    os.makedirs(binaries_dir, exist_ok=True)
                    for file_resource in binaries:
                        accessed = parsedate(file_resource.headers['Date'])
                        modified = parsedate(file_resource.headers['Last-Modified'])
                        file = file_resource.describe(PCDMFile)

                        binary_filename = os.path.join(binaries_dir, str(file.filename))
                        with open(binary_filename, mode='wb') as binary:
                            with file_resource.open() as stream:
                                for chunk in stream:
                                    binary.write(chunk)

                        # update the atime and mtime of the file to reflect the time of the
                        # HTTP request and the resource's last-modified time in the repo
                        os.utime(binary_filename, times=(mktime(accessed), mktime(modified)))
                        logger.debug(f'Copied {file.uri} to {binary.name}')

                count['exported'] += 1

            except DataReadError as e:
                # log the failure, but continue to attempt to export the rest of the URIs
                logger.error(f'Export of {uri} failed: {e}')
                count['errors'] += 1
            except (ClientError, ConnectionError) as e:
                # log the failure, but continue to attempt to export the rest of the URIs
                logger.error(f'Unable to retrieve {uri}: {e}')
                count['errors'] += 1

            # update the status
            yield {
                'time': timer.now(),
                'count': count,
            }

        try:
            serializer.finish()
        except EmptyItemListError:
            logger.error("No items could be exported; skipping writing file")

        logger.info(f'Exported {count["exported"]} of {count["total"]} items')

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
            'type': 'export_complete' if count["exported"] == count["total"] else 'partial_export',
            'content_type': serializer.content_type,
            'file_extension': serializer.file_extension,
            'count': count,
        }
