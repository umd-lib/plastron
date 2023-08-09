import csv
import logging
import os
import re
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate
from enum import Enum
from os.path import splitext, basename, normpath, relpath
from pathlib import Path
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from time import mktime
from typing import List, Mapping, Optional
from urllib.parse import urlsplit
from zipfile import ZipFile

import yaml
from bagit import make_bag
from paramiko import SFTPClient, SSHException
from rdflib import URIRef
from rdflib.util import from_n3
from requests import ConnectionError

import plastron.models
from plastron.client import Client, ClientError
from plastron.core.exceptions import DataReadException, FailureException
from plastron.core.util import ItemLog, datetimestamp
from plastron.files import get_ssh_client
from plastron.models import Item
from plastron.namespaces import get_manager
from plastron.rdf.pcdm import File
from plastron.rdf.rdf import RDFDataProperty, Resource
from plastron.serializers import CSVSerializer, SERIALIZER_CLASSES, detect_resource_class, EmptyItemListError

nsm = get_manager()
logger = logging.getLogger(__name__)
UUID_REGEX = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)


class ModelClassNotFoundError(Exception):
    def __init__(self, model_name: str, *args):
        super().__init__(*args)
        self.model_name = model_name


def build_lookup_index(item: Resource, index_string: str):
    """
    Build a lookup dictionary for embedded object properties of an item.

    :param item:
    :param index_string:
    :return:
    """
    index = defaultdict(dict)
    if index_string is None:
        return index

    pattern = r'([\w]+)\[(\d+)\]'
    for entry in index_string.split(';'):
        key, uriref = entry.split('=')
        m = re.search(pattern, key)
        attr = m[1]
        i = int(m[2])
        prop = getattr(item, attr)
        try:
            index[attr][i] = prop[URIRef(item.uri + uriref)]
        except IndexError:
            # need to create an object with that URI
            obj = prop.obj_class(uri=URIRef(item.uri + uriref))
            # TODO: what if i > 0?
            prop.values.append(obj)
            index[attr][i] = obj
    return index


def build_fields(fieldnames, model_class):
    property_attrs = {header: attrs for attrs, header in model_class.HEADER_MAP.items()}
    fields = defaultdict(list)
    # group typed and language-tagged columns by their property attribute
    for header in fieldnames:
        if '[' in header:
            # this field has a language tag
            # header format is "Header Label [Language Label]"
            header_label, language_label = re.search(r'^([^[]+)\s+\[(.+)]$', header).groups()
            try:
                attrs = property_attrs[header_label]
            except KeyError as e:
                raise DataReadException(f'Unknown header "{header}" in import file.') from e
            # if the language label isn't a name in the LANGUAGE_CODES table,
            # assume that it is itself a language code
            lang_code = CSVSerializer.LANGUAGE_CODES.get(language_label, language_label)
            fields[attrs].append({
                'header': header,
                'lang_code': lang_code,
                'datatype': None
            })
        elif '{' in header:
            # this field has a datatype
            # header format is "Header Label {Datatype Label}
            header_label, datatype_label = re.search(r'^([^{]+)\s+{(.+)}$', header).groups()
            try:
                attrs = property_attrs[header_label]
            except KeyError as e:
                raise DataReadException(f'Unknown header "{header}" in import file.') from e
            # the datatype label should either be a key in the lookup table,
            # or an n3-abbreviated URI of a datatype
            try:
                datatype_uri = CSVSerializer.DATATYPE_URIS.get(datatype_label, from_n3(datatype_label, nsm=nsm))
                if not isinstance(datatype_uri, URIRef):
                    raise DataReadException(f'Unknown datatype "{datatype_label}" in "{header}" in import file.')
            except KeyError as e:
                raise DataReadException(f'Unknown datatype "{datatype_label}" in "{header}" in import file.') from e

            fields[attrs].append({
                'header': header,
                'lang_code': None,
                'datatype': datatype_uri
            })
        else:
            # no language tag or datatype
            # make sure we skip the system columns
            if header not in CSVSerializer.SYSTEM_HEADERS:
                if header not in property_attrs:
                    raise DataReadException(f'Unrecognized header "{header}" in import file.')
                # check for a default datatype defined in the model
                attrs = property_attrs[header]
                prop = model_class.name_to_prop.get(attrs)
                if prop is not None and issubclass(prop, RDFDataProperty):
                    datatype_uri = prop.datatype
                else:
                    datatype_uri = None
                fields[attrs].append({
                    'header': header,
                    'lang_code': None,
                    'datatype': datatype_uri
                })
    return fields


def is_run_dir(path: Path) -> bool:
    return path.is_dir() and re.match(r'^\d{14}$', path.name)


class JobError(Exception):
    def __init__(self, job, *args):
        super().__init__(*args)
        self.job = job

    def __str__(self):
        return f'Job {self.job} error: {super().__str__()}'


class ConfigMissingError(JobError):
    pass


class MetadataError(JobError):
    pass


class ImportedItemStatus(Enum):
    CREATED = 'created'
    MODIFIED = 'modified'
    UNCHANGED = 'unchanged'


class ImportJob:
    def __init__(self, job_id, jobs_dir):
        self.id = job_id
        # URL-escaped ID that can be used as a path segment on a filesystem or URL
        self.safe_id = urllib.parse.quote(job_id, safe='')
        # use a timestamp to differentiate different runs of the same import job
        self.run_timestamp = datetimestamp()
        self.dir = Path(jobs_dir) / self.safe_id
        self.config_filename = self.dir / 'config.yml'
        self.metadata_filename = self.dir / 'source.csv'
        self.config = {}
        self._model_class = None

        # record of items that are successfully loaded
        completed_fieldnames = ['id', 'timestamp', 'title', 'uri', 'status']
        self.completed_log = ItemLog(self.dir / 'completed.log.csv', completed_fieldnames, 'id')

    def __str__(self):
        return self.id

    def __getattr__(self, item):
        try:
            return self.config[item]
        except KeyError:
            raise AttributeError(f'No attribute or config key named {item} found')

    @property
    def dir_exists(self):
        return self.dir.is_dir()

    def load_config(self):
        try:
            with open(self.config_filename) as config_file:
                config = yaml.safe_load(config_file)
            if config is None:
                raise ConfigMissingError(self, f'Config file {self.config_filename} is empty')
            self.config = config
            return self.config
        except FileNotFoundError as e:
            raise ConfigMissingError(self, f'Config file {self.config_filename} is missing') from e

    def save_config(self, config):
        # store the relevant config
        self.config = config

        # if we are not resuming, make sure the directory exists
        os.makedirs(self.dir, exist_ok=True)
        with open(self.config_filename, mode='w') as config_file:
            yaml.dump(
                stream=config_file,
                data={'job_id': self.id, **{k: str(v) for k, v in self.config.items()}}
            )

    def store_metadata_file(self, input_file):
        with open(self.metadata_filename, mode='w') as file:
            copyfileobj(input_file, file)
            logger.debug(f"Copied input file {getattr(input_file, 'name', '<>')} to {file.name}")

    @property
    def model_class(self):
        if self._model_class is None:
            try:
                self._model_class = getattr(plastron.models, self.model)
            except AttributeError as e:
                raise ModelClassNotFoundError(self.model) from e
        return self._model_class

    def complete(self, item: Resource, line_reference: str, status: ImportedItemStatus):
        # write to the completed item log
        self.completed_log.append({
            'id': getattr(item, 'identifier', line_reference),
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(item, 'title', ''),
            'uri': getattr(item, 'uri', ''),
            'status': status.value
        })

    def metadata(self, **kwargs):
        return MetadataRows(self, **kwargs)

    def new_run(self):
        return ImportRun(self)

    def get_run(self, timestamp=None):
        if timestamp is None:
            # get the latest run
            return self.latest_run()
        else:
            return ImportRun(self).load(timestamp)

    @property
    def runs(self):
        return sorted((d.name for d in filter(is_run_dir, self.dir.iterdir())), reverse=True)

    def latest_run(self):
        try:
            return ImportRun(self).load(self.runs[0])
        except IndexError:
            return None


DROPPED_INVALID_FIELDNAMES = ['id', 'timestamp', 'title', 'uri', 'reason']
DROPPED_FAILED_FIELDNAMES = ['id', 'timestamp', 'title', 'uri', 'reason']


class ImportRun:
    """
    A single run of an import job. Records the logs of invalid and failed items (if any).
    """
    def __init__(self, job: ImportJob):
        self.job = job
        self.dir = None
        self.timestamp = None
        self._invalid_items = None
        self._failed_items = None

    def load(self, timestamp: str):
        """
        Load an existing import run by its timestamp.

        :param timestamp: should be 14 digits expressing YYYYMMDDHHMMSS
        :return:
        """
        self.timestamp = timestamp
        self.dir = self.job.dir / self.timestamp
        if not self.dir.is_dir():
            raise FailureException(f'Import run {self.timestamp} not found')
        return self

    @property
    def invalid_items(self) -> ItemLog:
        """
        Log of items that failed metadata validation during this import run.
        """
        if self._invalid_items is None:
            self._invalid_items = ItemLog(self.dir / 'dropped-invalid.log.csv', DROPPED_INVALID_FIELDNAMES, 'id')
        return self._invalid_items

    @property
    def failed_items(self) -> ItemLog:
        """
        Log of items that failed when loading into the repository during this import run.
        """
        if self._failed_items is None:
            self._failed_items = ItemLog(self.dir / 'dropped-failed.log.csv', DROPPED_FAILED_FIELDNAMES, 'id')
        return self._failed_items

    def start(self):
        """
        Sets the timestamp for this run, and creates the log directory for it.

        :return:
        """
        if self.dir is not None:
            raise FailureException('Run completed, cannot start again')
        self.timestamp = datetimestamp()
        self.dir = self.job.dir / self.timestamp
        os.makedirs(self.dir)
        return self

    def drop_failed(self, item, line_reference, reason=''):
        """
        Add the item to the log of failed items for this run.

        :param item: the failed item
        :param line_reference: string in the form <filename>:<line number>
        :param reason: cause of the failure; usually the message from the underlying exception
        :return:
        """
        logger.warning(
            f'Dropping failed {line_reference} from import job "{self.job}" run {self.timestamp}: {reason}'
        )
        self.failed_items.append({
            'id': getattr(item, 'identifier', line_reference),
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(item, 'title', ''),
            'uri': getattr(item, 'uri', ''),
            'reason': reason
        })

    def drop_invalid(self, item, line_reference, reason=''):
        """
        Add the item to the log of invalid items for this run.

        :param item: the invalid item
        :param line_reference: string in the form <filename>:<line number>
        :param reason: validation failure message(s)
        :return:
        """
        logger.warning(
            f'Dropping invalid {line_reference} from import job "{self.job}" run {self.timestamp}: {reason}'
        )
        self.invalid_items.append({
            'id': getattr(item, 'identifier', line_reference),
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(item, 'title', ''),
            'uri': getattr(item, 'uri', ''),
            'reason': reason
        })

    def complete(self, item, line_reference, status):
        """
        Delegates to the `plastron.jobs.ImportJob.complete()` method.

        :param item:
        :param line_reference:
        :param status:
        :return:
        """
        self.job.complete(item, line_reference, status)


class MetadataRows:
    """
    Iterable sequence of rows from the metadata CSV file of an import job.
    """
    def __init__(self, job: ImportJob, limit: int = None, percentage: int = None):
        self.job = job
        self.limit = limit
        self.metadata_file = None

        try:
            self.metadata_file = open(job.metadata_filename, 'r')
        except FileNotFoundError as e:
            raise MetadataError(job, f'Cannot read source file "{job.metadata_filename}: {e}') from e

        self.csv_file = csv.DictReader(self.metadata_file)

        try:
            self.fields = build_fields(self.fieldnames, self.model_class)
        except DataReadException as e:
            raise FailureException(str(e)) from e

        self.validation_reports: List[Mapping] = []
        self.skipped = 0
        self.subset_to_load = None

        self.total = None
        self.rows = 0
        self.errors = 0
        self.valid = 0
        self.invalid = 0
        self.created = 0
        self.updated = 0
        self.unchanged = 0
        self.files = 0

        if self.metadata_file.seekable():
            # get the row count of the file, then rewind the CSV file
            self.total = sum(1 for _ in self.csv_file)
            self._rewind_csv_file()
        else:
            # file is not seekable, so we can't get a row count in advance
            self.total = None

        if percentage is not None:
            if not self.metadata_file.seekable():
                raise FailureException('Cannot execute a percentage load using a non-seekable file')
            identifier_column = self.model_class.HEADER_MAP['identifier']
            identifiers = [
                row[identifier_column] for row in self.csv_file if row[identifier_column] not in job.completed_log
            ]
            self._rewind_csv_file()

            if len(identifiers) == 0:
                logger.info('No items remaining to load')
                self.subset_to_load = []
            else:
                target_count = int(((percentage / 100) * self.total))
                logger.info(f'Attempting to load {target_count} items ({percentage}% of {self.total})')
                if len(identifiers) > target_count:
                    # evenly space the items to load among the remaining items
                    step_size = int((100 * (1 - (len(job.completed_log) / self.total))) / percentage)
                else:
                    # load all remaining items
                    step_size = 1
                self.subset_to_load = identifiers[::step_size]

    def _rewind_csv_file(self):
        # rewind the file and re-create the CSV reader
        self.metadata_file.seek(0)
        self.csv_file = csv.DictReader(self.metadata_file)

    @property
    def model_class(self):
        return self.job.model_class

    @property
    def has_binaries(self):
        return 'FILES' in self.fieldnames

    @property
    def fieldnames(self):
        return self.csv_file.fieldnames

    @property
    def identifier_column(self):
        return self.model_class.HEADER_MAP['identifier']

    def stats(self):
        return {
            'total': self.total,
            'rows': self.rows,
            'errors': self.errors,
            'valid': self.valid,
            'invalid': self.invalid,
            'created': self.created,
            'updated': self.updated,
            'unchanged': self.unchanged,
            'files': self.files
        }

    def __iter__(self):
        for row_number, line in enumerate(self.csv_file, 1):
            if self.limit is not None and row_number > self.limit:
                logger.info(f'Stopping after {self.limit} rows')
                break

            if self.subset_to_load is not None and line[self.identifier_column] not in self.subset_to_load:
                continue

            line_reference = f"{self.job.metadata_filename}:{row_number + 1}"
            logger.debug(f'Processing {line_reference}')
            self.rows += 1

            if any(v is None for v in line.values()):
                self.errors += 1
                self.validation_reports.append({
                    'line': line_reference,
                    'is_valid': False,
                    'error': f'Line {line_reference} has the wrong number of columns'
                })
                self.job.drop_invalid(item=None, line_reference=line_reference, reason='Wrong number of columns')
                continue

            row = Row(line_reference, row_number, line, self.identifier_column)

            if row.identifier in self.job.completed_log:
                logger.info(f'Already loaded "{row.identifier}" from {line_reference}, skipping')
                self.skipped += 1
                continue

            yield row

        if self.total is None:
            # if we weren't able to get the total count before,
            # use the final row count as the total count for the
            # job completion message
            self.total = self.rows


class Row:
    def __init__(self, line_reference: str, row_number: int, data: Mapping, identifier_column: str):
        self.line_reference = line_reference
        self.number = row_number
        self.data = data
        self.identifier_column = identifier_column

    def __getitem__(self, item):
        return self.data[item]

    def get(self, key, default=None):
        return self.data.get(key, default)

    @property
    def identifier(self):
        return self.data[self.identifier_column]

    @property
    def has_uri(self):
        return 'URI' in self.data and self.data['URI'].strip() != ''

    @property
    def uri(self) -> URIRef:
        return URIRef(self.data['URI']) if self.has_uri else None

    @property
    def has_files(self):
        return 'FILES' in self.data and self.data['FILES'].strip() != ''

    @property
    def has_item_files(self):
        return 'ITEM_FILES' in self.data and self.data['ITEM_FILES'].strip() != ''

    @property
    def filenames(self):
        return self.data['FILES'].strip().split(';') if self.has_files else []

    @property
    def index_string(self):
        return self.data.get('INDEX')


@dataclass
class ExportJob:
    client: Client
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
                    raise DataReadException(f'No UUID found in {uri}')
                item_dir = match[0]

                _, graph = self.client.get_graph(uri)
                model_class = detect_resource_class(graph, uri, fallback=Item)
                obj = model_class.from_graph(graph, uri)
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

            except DataReadException as e:
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
