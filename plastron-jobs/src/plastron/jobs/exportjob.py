import logging
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate
from os.path import basename, splitext
from pathlib import Path
from tempfile import TemporaryDirectory
from time import mktime
from typing import Any, Generator, Iterator, Optional
from urllib.parse import urlsplit
from zipfile import ZipFile

from bagit import make_bag
from paramiko import SFTPClient, SSHException
from requests import ConnectionError

from plastron.client import ClientError
from plastron.context import PlastronContext
from plastron.files import get_ssh_client, FileSpec, get_usage_tag
from plastron.jobs import Job
from plastron.models.pcdm import PCDMFile, PCDMObject
from plastron.models.umd import Item
from plastron.repo import DataReadError
from plastron.repo.aggregation import AggregationResource
from plastron.repo.pcdm import PCDMFileBearingResource, PCDMObjectResource, PCDMPageResource
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
    bag_root = Path(root_dirname)
    with ZipFile(dest, mode='w') as zip_file:
        for dirpath, _, filenames in os.walk(bag.path):
            for name in filenames:
                src_filename = Path(dirpath, name)
                archived_name = bag_root / src_filename.relative_to(bag.path)
                zip_file.write(filename=src_filename, arcname=archived_name)


def gather_page_files(
    resource: AggregationResource,
    mime_type: str = None,
    binaries_dir: str = None,
) -> Iterator[FileSpec]:
    """Returns an iterator of `FileSpec` objects representing each file for
    each page in the given `resource`."""

    for page_resource in resource.get_sequence(PCDMPageResource):
        page_obj = page_resource.read().describe(PCDMObject)
        page_label = page_obj.title.value

        yield from gather_files(
            resource=page_resource,
            mime_type=mime_type,
            label=page_label,
            binaries_dir=binaries_dir,
        )


def gather_files(
    resource: PCDMFileBearingResource,
    label: str = None,
    mime_type: str = None,
    binaries_dir: str = None,
) -> Iterator[FileSpec]:
    """Returns an iterator of `FileSpec` objects representing each file of
    the given `resource`."""

    binaries_path = Path(binaries_dir) if binaries_dir else None
    for file_resource in resource.get_files(mime_type=mime_type):
        file_obj = file_resource.describe(PCDMFile)
        filename = file_obj.filename.value
        if binaries_path:
            filename = str(binaries_path / filename)
        yield FileSpec(
            name=filename,
            label=label,
            source=file_resource,
            usage=get_usage_tag(file_obj),
        )


class Stopwatch:
    def __init__(self):
        self._start = datetime.now().timestamp()

    def now(self) -> dict[str, float]:
        now = datetime.now().timestamp()
        return {
            'started': self._start,
            'now': now,
            'elapsed': now - self._start
        }


class FileSize:
    def __init__(self, size: int) -> None:
        self._size = size

    def __str__(self) -> str:
        return ' '.join(str(x) for x in format_size(self._size, decimal_places=2))


@dataclass
class ExportJob(Job):
    context: PlastronContext
    export_format: str
    export_binaries: bool
    binary_types: str
    output_dest: str
    uri_template: str
    uris: list[str]
    key: str

    def __post_init__(self):
        if self.binary_types:
            accepted_types = self.binary_types.split(',')

            # filter files by their MIME type
            def mime_type_filter(file_spec: FileSpec) -> bool:
                return str(file_spec.source.headers['Content-Type']) in accepted_types

            self.mime_type_filter = mime_type_filter
        else:
            # default filter is None; in this case filter() will return
            # all items that evaluate to true
            self.mime_type_filter = None

    def get_page_files(self, resource: PCDMObjectResource, item_dir: str = None) -> tuple[list[FileSpec], FileSize]:
        if not self.export_binaries:
            return [], FileSize(0)

        logger.info(f'Gathering binaries for the pages of {resource.url}')

        files = list(filter(self.mime_type_filter, gather_page_files(resource, mime_type=None, binaries_dir=item_dir)))
        total_size = FileSize(sum(f.source.size for f in files))
        logger.info(f'Total size of page member files: {total_size}')

        return files, total_size

    def get_item_files(self, resource: PCDMObjectResource, item_dir: str = None) -> tuple[list[FileSpec], FileSize]:
        if not self.export_binaries:
            return [], FileSize(0)

        logger.info(f'Gathering item-level binaries for {resource.url}')

        files = list(filter(self.mime_type_filter, gather_files(resource, mime_type=None, binaries_dir=item_dir)))
        total_size = FileSize(sum(f.source.size for f in files))
        logger.info(f'Total size of item-level files: {total_size}')

        return files, total_size

    def run(self) -> Generator[dict[str, Any], None, dict[str, Any]]:
        logger.info(f'Requested export format is {self.export_format}')
        if self.export_binaries:
            types = self.binary_types or '*/*'
            logger.info(f'Exporting binaries with types: {types}')

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
        logger.debug(f'Assembling export bag in {temp_dir.name}')
        bag = make_bag(temp_dir.name)

        export_dir = os.path.join(temp_dir.name, 'data')
        serializer = serializer_class(directory=export_dir)
        yield {
            'time': timer.now(),
            'count': count,
            'state': 'in_progress',
            'progress': 0,
        }
        for n, uri in enumerate(self.uris, 1):
            try:
                logger.info(f'Exporting item {count["exported"] + 1}/{count["total"]}: {uri}')

                resource = self.context.repo[uri:PCDMObjectResource].read()
                # use a translated version of the repo path as the default item directory name
                # e.g., "/dc/2023/1/de/84/37/0d/de84370d-f90a-444f-a87f-dd79e0438884" becomes
                # "dc.2023.1.de.84.37.0d.de84370d-f90a-444f-a87f-dd79e0438884"
                item_dir = resource.path.lstrip('/').replace('/', '.')

                model_class = detect_resource_class(resource.graph, resource.url, fallback=Item)

                # write the metadata for this object
                obj = resource.describe(model=model_class)
                # use the identifier field from the model as a better item directory name
                if hasattr(obj, 'identifier'):
                    item_dir = str(obj.identifier.value or item_dir)

                page_files, page_files_size = self.get_page_files(resource, item_dir=item_dir)
                item_files, item_files_size = self.get_item_files(resource, item_dir=item_dir)
                serializer.write(
                    obj,
                    files=page_files,
                    item_files=item_files,
                    public_url=self.context.get_public_url(resource),
                )

                # Write binary files for page member and item-level files
                all_files = [*page_files, *item_files]

                if all_files:
                    binaries_dir = Path(export_dir, item_dir)
                    binaries_dir.mkdir(parents=True, exist_ok=True)
                    for file_spec in all_files:
                        file_resource = file_spec.source
                        accessed = parsedate(file_resource.headers['Date'])
                        modified = parsedate(file_resource.headers['Last-Modified'])
                        file = file_resource.describe(PCDMFile)

                        binary_filename = binaries_dir / str(file.filename)
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
                'state': 'in_progress',
                'progress': int(n / count['total'] * 100),
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

        state = 'export_complete' if count['exported'] == count['total'] else 'partial_export'
        return {
            'type': state,
            'content_type': serializer.content_type,
            'file_extension': serializer.file_extension,
            'count': count,
            'state': state,
            'progress': 100,
        }
