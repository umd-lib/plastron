import csv
import dataclasses
import logging
import os
import re
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate
from enum import Enum
from os.path import splitext, basename, normpath, relpath
from pathlib import Path
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from time import mktime
from typing import List, Mapping, Optional, Type, Union, Any, IO, NamedTuple
from urllib.parse import urlsplit
from zipfile import ZipFile

import yaml
from bagit import make_bag
from paramiko import SFTPClient, SSHException
from rdflib import URIRef, Literal
from requests import ConnectionError

from plastron.client import Client, ClientError
from plastron.utils import datetimestamp, strtobool, ItemLog
from plastron.files import get_ssh_client, ZipFileSource, RemoteFileSource, HTTPFileSource, LocalFileSource, \
    BinarySource
from plastron.jobs.utils import create_repo_changeset, build_file_groups, annotate_from_files, build_fields, \
    RepoChangeset, ColumnSpec, parse_value_string
from plastron.models import get_model_class, Item, ModelClassNotFoundError
from plastron.rdf.pcdm import File, PreservationMasterFile, Object
from plastron.rdf.rdf import Resource
from plastron.rdfmapping.properties import ValidationFailure
from plastron.repo import Repository, DataReadError
from plastron.serializers import SERIALIZER_CLASSES, detect_resource_class, EmptyItemListError
from plastron.validation import ValidationError

logger = logging.getLogger(__name__)
UUID_REGEX = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)


class LineReference(NamedTuple):
    filename: str
    line_number: int

    def __str__(self):
        return f'{self.filename}:{self.line_number}'


def is_run_dir(path: Path) -> bool:
    return path.is_dir() and re.match(r'^\d{14}$', path.name)


class JobError(Exception):
    def __init__(self, job, *args):
        super().__init__(*args)
        self.job = job

    def __str__(self):
        return f'Job {self.job} error: {super().__str__()}'


class JobConfigError(JobError):
    pass


class MetadataError(JobError):
    pass


class ImportedItemStatus(Enum):
    CREATED = 'created'
    MODIFIED = 'modified'
    UNCHANGED = 'unchanged'


@dataclass
class ImportConfig:
    job_id: str
    model: Optional[str] = None
    access: Optional[str] = None
    member_of: Optional[str] = None
    container: Optional[str] = None
    binaries_location: Optional[str] = None
    extract_text_types: Optional[str] = None

    @classmethod
    def from_file(cls, filename: Union[str, Path]) -> 'ImportConfig':
        try:
            with open(filename) as file:
                config = yaml.safe_load(file)
        except FileNotFoundError as e:
            raise JobConfigError(f'Config file {filename} is missing') from e
        if config is None:
            raise JobConfigError(f'Config file {filename} is empty')
        for key, value in config.items():
            # catch any improperly serialized "None" values, and convert to None
            if value == 'None':
                config[key] = None
        return cls(**config)

    def save(self, filename: Union[str, Path]):
        config = {k: str(v) if v is not None else v for k, v in vars(self).items()}
        with open(filename, mode='w') as file:
            yaml.dump(data=config, stream=file)


class ImportJob:
    def __init__(self, job_id, jobs_dir, repo: Repository, ssh_private_key: str = None):
        self.id = job_id
        # URL-escaped ID that can be used as a path segment on a filesystem or URL
        self.safe_id = urllib.parse.quote(job_id, safe='')
        # use a timestamp to differentiate different runs of the same import job
        self.run_timestamp = datetimestamp()
        self.dir = Path(jobs_dir) / self.safe_id
        self.config_filename = self.dir / 'config.yml'
        self.metadata_filename = self.dir / 'source.csv'
        self.config = ImportConfig(job_id=job_id)
        self._model_class = None

        # record of items that are successfully loaded
        completed_fieldnames = ['id', 'timestamp', 'title', 'uri', 'status']
        self.completed_log = ItemLog(self.dir / 'completed.log.csv', completed_fieldnames, 'id')

        self.repo = repo
        self.ssh_private_key = ssh_private_key

    def __str__(self):
        return self.id

    @property
    def dir_exists(self) -> bool:
        return self.dir.is_dir()

    def load_config(self) -> ImportConfig:
        self.config = ImportConfig.from_file(self.config_filename)
        return self.config

    def save_config(self, config: Mapping[str, Any]):
        # store the relevant config
        self.config = dataclasses.replace(self.config, **config)

        # if we are not resuming, make sure the directory exists
        os.makedirs(self.dir, exist_ok=True)
        self.config.save(self.config_filename)

    def store_metadata_file(self, input_file: IO):
        with open(self.metadata_filename, mode='w') as file:
            copyfileobj(input_file, file)
            logger.debug(f"Copied input file {getattr(input_file, 'name', '<>')} to {file.name}")

    @property
    def model_class(self):
        if self._model_class is None:
            self._model_class = get_model_class(self.config.model)
        return self._model_class

    def complete(self, item: Resource, line_reference: LineReference, status: ImportedItemStatus):
        # write to the completed item log
        self.completed_log.append({
            'id': getattr(item, 'identifier', str(line_reference)),
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(item, 'title', ''),
            'uri': getattr(item, 'uri', ''),
            'status': status.value
        })

    def metadata(self, **kwargs) -> 'MetadataRows':
        return MetadataRows(self, **kwargs)

    def new_run(self) -> 'ImportRun':
        return ImportRun(self)

    def get_run(self, timestamp: Optional[str] = None) -> 'ImportRun':
        if timestamp is None:
            # get the latest run
            return self.latest_run()
        else:
            return ImportRun(self).load(timestamp)

    @property
    def runs(self) -> List[str]:
        return sorted((d.name for d in filter(is_run_dir, self.dir.iterdir())), reverse=True)

    def latest_run(self) -> Optional['ImportRun']:
        try:
            return ImportRun(self).load(self.runs[0])
        except IndexError:
            return None

    def start(
            self,
            model: Type[Resource],
            access: URIRef = None,
            member_of: URIRef = None,
            container: str = None,
            binaries_location: str = None,
            extract_text_types: str = None,
            **kwargs,
    ):
        self.save_config({
            'model': model,
            'access': access,
            'member_of': member_of,
            'container': container,
            'binaries_location': binaries_location,
            'extract_text_types': extract_text_types,
        })

        return (yield from self.import_items(**kwargs))

    def resume(self, **kwargs):
        if not self.dir_exists:
            raise RuntimeError(f'Cannot resume job "{self.id}": no such job directory: "{self.dir}"')

        logger.info(f'Resuming saved job {self.id}')
        try:
            # load stored config from the previous run of this job
            self.load_config()
        except FileNotFoundError:
            raise RuntimeError(f'Cannot resume job {self.id}: no config.yml found in {self.dir}')

        return (yield from self.import_items(**kwargs))

    def import_items(self, limit: int = None, percentage=None, validate_only: bool = False, import_file: IO = None):
        start_time = datetime.now().timestamp()

        if percentage:
            logger.info(f'Loading {percentage}% of the total items')
        if validate_only:
            logger.info('Validation-only mode, skipping imports')

        # if an import file was provided, save that as the new CSV metadata file
        if import_file is not None:
            self.store_metadata_file(import_file)

        try:
            metadata = self.metadata(limit=limit, percentage=percentage)
        except ModelClassNotFoundError as e:
            raise RuntimeError(f'Model class {e.model_name} not found') from e
        except JobError as e:
            raise RuntimeError(str(e)) from e

        if metadata.has_binaries and self.config.binaries_location is None:
            raise RuntimeError('Must specify --binaries-location if the metadata has a FILES column')

        initial_completed_item_count = len(self.completed_log)
        logger.info(f'Found {initial_completed_item_count} completed items')

        import_run = self.new_run().start()
        for row in metadata:
            repo_changeset = create_repo_changeset(self.repo.client, metadata, row)
            item = repo_changeset.item

            # count the number of files referenced in this row
            metadata.files += len(row.filenames)

            try:
                results = item.validate()
            except ValidationError as e:
                raise RuntimeError(f'Unable to run validation: {e}') from e

            is_valid = all(bool(result) for result in results.values())
            metadata.validation_reports.append({
                'line': row.line_reference,
                'is_valid': is_valid,
                # 'passed': [outcome for outcome in report.passed()],
                # 'failed': [outcome for outcome in report.failed()]
            })

            missing_files = [
                name for name in row.filenames if not self.get_source(self.config.binaries_location, name).exists()
            ]
            if len(missing_files) > 0:
                logger.warning(f'{len(missing_files)} file(s) for "{item}" not found')

            if is_valid and len(missing_files) == 0:
                metadata.valid += 1
                logger.info(f'"{item}" is valid')
            else:
                failures = {
                    getattr(item.__class__, name).label: result
                    for name, result in results.items()
                    if isinstance(result, ValidationFailure)
                }
                # drop invalid items
                metadata.invalid += 1
                logger.warning(f'"{item}" is invalid, skipping')
                reasons = [f'{name} {result}' for name, result in failures.items()]
                if len(missing_files) > 0:
                    reasons.extend(f'Missing file: {f}' for f in missing_files)
                import_run.drop_invalid(
                    item=item,
                    line_reference=row.line_reference,
                    reason=f'Validation failures: {"; ".join(reasons)}'
                )
                continue

            if validate_only:
                # validation-only mode
                continue

            try:
                self.update_repo(metadata, row, repo_changeset)
            except RuntimeError as e:
                metadata.errors += 1
                logger.error(f'{item} import failed: {e}')
                import_run.drop_failed(item, row.line_reference, reason=str(e))

            # update the status
            now = datetime.now().timestamp()
            yield {
                'time': {
                    'started': start_time,
                    'now': now,
                    'elapsed': now - start_time
                },
                'count': metadata.stats()
            }

        return metadata

    @property
    def access(self) -> Optional[URIRef]:
        if self.config.access is not None:
            return URIRef[self.config.access]
        else:
            return None

    @property
    def member_of(self) -> Optional[URIRef]:
        if self.config.member_of is not None:
            return URIRef(self.config.member_of)
        else:
            return None

    @property
    def extract_text_types(self) -> List[str]:
        if self.config.extract_text_types is not None:
            return self.config.extract_text_types.split(',')
        else:
            return []

    def update_repo(self, metadata: 'MetadataRows', row: 'Row', repo_changeset: RepoChangeset):
        """
        Updates the repository with the given RepoChangeSet

        :param metadata: A plastron.jobs.MetadataRows object representing the
                          CSV file being imported
        :param row: A single plastron.jobs.Row object representing the row
                     being imported
        :param repo_changeset: The RepoChangeSet object describing the changes
                                 to make to the repository.
        """
        created_uris = []
        updated_uris = []
        item = repo_changeset.item

        if item.uri == URIRef(''):
            # if an item is new, don't construct a SPARQL Update query
            # instead, just create and update normally
            # create new item in the repo
            logger.debug('Creating a new item')
            # add the access class
            if self.access is not None:
                item.rdf_type.append(self.access)
            # add the collection membership
            if self.member_of is not None:
                item.member_of = self.member_of

            if row.has_files:
                create_pages = bool(strtobool(row.get('CREATE_PAGES', 'True')))
                logger.debug('Adding pages and files to new item')
                self.add_files(
                    item,
                    build_file_groups(row['FILES']),
                    base_location=self.config.binaries_location,
                    access=self.access,
                    create_pages=create_pages
                )

            if row.has_item_files:
                self.add_files(
                    item,
                    build_file_groups(row['ITEM_FILES']),
                    base_location=self.config.binaries_location,
                    access=self.access,
                    create_pages=False
                )

            if self.extract_text_types is not None:
                annotate_from_files(item, self.extract_text_types)

            logger.debug(f"Creating resources in container: {self.config.container}")

            try:
                with self.repo.client.transaction() as txn_client:
                    item.create(txn_client, container_path=self.config.container)
                    item.update(txn_client)
            except Exception as e:
                raise RuntimeError(f'Creating item failed: {e}') from e

            self.complete(item, row.line_reference, ImportedItemStatus.CREATED)
            metadata.created += 1
            created_uris.append(item.uri)

        elif repo_changeset:
            # construct the SPARQL Update query if there are any deletions or insertions
            # then do a PATCH update of an existing item
            logger.info(f'Sending update for {item}')
            sparql_update = repo_changeset.build_sparql_update(self.repo.client)
            logger.debug(sparql_update)
            try:
                item.patch(self.repo.client, sparql_update)
            except ClientError as e:
                raise RuntimeError(f'Updating item failed: {e}') from e

            self.complete(item, row.line_reference, ImportedItemStatus.MODIFIED)
            metadata.updated += 1
            updated_uris.append(item.uri)

        else:
            self.complete(item, row.line_reference, ImportedItemStatus.UNCHANGED)
            metadata.unchanged += 1
            logger.info(f'No changes found for "{item}" ({row.uri}); skipping')
            metadata.skipped += 1

    def get_source(self, base_location: str, path: str) -> BinarySource:
        """
        Get an appropriate BinarySource based on the type of ``base_location``.
        The following forms of ``base_location`` are recognized:

        * ``zip:<path to zipfile>``
        * ``sftp:<user>@<host>/<path to dir>``
        * ``http://<host>/<path to dir>``
        * ``zip+sftp:<user>@<host>/<path to zipfile>``
        * ``<local dir path>``

        :param base_location:
        :param path:
        :return:
        """
        if base_location.startswith('zip:'):
            return ZipFileSource(base_location[4:], path)
        elif base_location.startswith('sftp:'):
            return RemoteFileSource(
                location=os.path.join(base_location, path),
                ssh_options={'key_filename': self.ssh_private_key}
            )
        elif base_location.startswith('http:') or base_location.startswith('https:'):
            base_uri = base_location if base_location.endswith('/') else base_location + '/'
            return HTTPFileSource(base_uri + path)
        elif base_location.startswith('zip+sftp:'):
            return ZipFileSource(
                zip_file=base_location[4:],
                path=path,
                ssh_options={'key_filename': self.ssh_private_key}
            )
        else:
            # with no URI prefix, assume a local file path
            return LocalFileSource(localpath=os.path.join(base_location, path))

    def get_file(self, base_location: str, filename: str) -> File:
        """
        Get a file object for the given base_location and filename.

        Currently, if the file has an "image/tiff" MIME type, this method returns
        a :py:class:`plastron.pcdm.PreservationMasterFile`; otherwise it returns
        a basic :py:class:`plastron.pcdm.File`.

        :param base_location:
        :param filename:
        :return:
        """
        source = self.get_source(base_location, filename)

        # XXX: hardcoded image/tiff as the preservation master format
        # TODO: make preservation master format configurable per collection or job
        if source.mimetype() == 'image/tiff':
            file_class = PreservationMasterFile
        else:
            file_class = File

        return file_class.from_source(title=basename(filename), source=source)

    def add_files(
            self,
            item: Object,
            file_groups: Mapping[str, Any],
            base_location: str,
            access: Optional[URIRef] = None,
            create_pages=True,
    ) -> int:
        """
        Add pages and files to the given item. A page is added for each key (basename) in the file_groups
        parameter, and a file is added for each element in the value list for that key.

        :param item: PCDM Object to add the pages to.
        :param file_groups: Dictionary of basename to filename list mappings.
        :param base_location: Location of the files.
        :param access: Optional RDF class representing the access level for this item.
        :param create_pages: Whether to create an intermediate page object for each file group. Defaults to True.
        :return: The number of files added.
        """
        if base_location is None:
            raise RuntimeError('Must specify a binaries-location')

        if create_pages:
            logger.debug(f'Creating {len(file_groups.keys())} page(s)')

        count = 0

        for n, (rootname, filenames) in enumerate(file_groups.items(), 1):
            if create_pages:
                # create a member object for each rootname
                # delegate to the item model how to build the member object
                member = item.get_new_member(rootname, n)
                # add to the item
                item.add_member(member)
                proxy = item.append_proxy(member, title=member.title)
                # add the access class to the member resources
                if access is not None:
                    member.rdf_type.append(access)
                    proxy.rdf_type.append(access)
                file_parent = member
            else:
                # files will be added directly to the item
                file_parent = item

            # add the files to their parent object (either the item or a member)
            for filename in filenames:
                file = self.get_file(base_location, filename)
                count += 1
                file_parent.add_file(file)
                if access is not None:
                    file.rdf_type.append(access)

        return count


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
            raise RuntimeError(f'Import run {self.timestamp} not found')
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
            raise RuntimeError('Run completed, cannot start again')
        self.timestamp = datetimestamp()
        self.dir = self.job.dir / self.timestamp
        self.dir.mkdir(parents=True, exist_ok=True)
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
        except DataReadError as e:
            raise RuntimeError(str(e)) from e

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
                raise RuntimeError('Cannot execute a percentage load using a non-seekable file')
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
    def has_binaries(self) -> bool:
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

            line_reference = LineReference(filename=str(self.job.metadata_filename), line_number=row_number + 1)
            logger.debug(f'Processing {line_reference}')
            self.rows += 1

            if any(v is None for v in line.values()):
                self.errors += 1
                self.validation_reports.append({
                    'line': line_reference,
                    'is_valid': False,
                    'error': f'Line {line_reference} has the wrong number of columns'
                })
                # TODO: this should be part of ImportRun?
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
    def __init__(self, line_reference: LineReference, row_number: int, data: Mapping, identifier_column: str):
        self.line_reference = line_reference
        self.number = row_number
        self.data = data
        self.identifier_column = identifier_column

    def __getitem__(self, item):
        return self.data[item]

    def get(self, key, default=None):
        return self.data.get(key, default)

    def parse_value(self, column: ColumnSpec) -> List[Union[Literal, URIRef]]:
        return parse_value_string(self[column.header], column)

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
                    raise DataReadError(f'No UUID found in {uri}')
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
