import dataclasses
import logging
import os
import re
import urllib.parse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate
from enum import Enum
from os.path import splitext, basename, normpath, relpath
from pathlib import Path
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from time import mktime
from typing import List, Mapping, Optional, Type, Union, Any, IO, Generator, Iterable
from urllib.parse import urlsplit
from zipfile import ZipFile

import yaml
from bagit import make_bag
from paramiko import SFTPClient, SSHException
from plastron.utils import datetimestamp, ItemLog
from rdflib import URIRef, Literal
from requests import ConnectionError

from plastron.client import Client, ClientError, random_slug
from plastron.files import get_ssh_client, ZipFileSource, RemoteFileSource, HTTPFileSource, LocalFileSource, \
    BinarySource
from plastron.jobs.utils import get_item_to_import, build_file_groups, annotate_from_files, build_fields, \
    RepoChangeset, ColumnSpec, parse_value_string, Row, ImportSpreadsheet, JobError, JobConfigError, LineReference
from plastron.models import get_model_class, Item, ModelClassNotFoundError, umdform
from plastron.models.umd import Page, Proxy, PCDMObject, PCDMFile
from plastron.rdf.pcdm import File, PreservationMasterFile, Object
from plastron.rdf.rdf import Resource
from plastron.repo import Repository, DataReadError, ContainerResource, BinaryResource, RDFResourceType
from plastron.rdfmapping.validation import ValidationResultsDict, ValidationResult, ValidationSuccess, ValidationFailure
from plastron.serializers import SERIALIZER_CLASSES, detect_resource_class, EmptyItemListError
from plastron.validation import ValidationError

logger = logging.getLogger(__name__)
UUID_REGEX = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)

DROPPED_INVALID_FIELDNAMES = ['id', 'timestamp', 'title', 'uri', 'reason']
DROPPED_FAILED_FIELDNAMES = ['id', 'timestamp', 'title', 'uri', 'reason']


def is_run_dir(path: Path) -> bool:
    return path.is_dir() and re.match(r'^\d{14}$', path.name)


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


def get_new_member(item, rootname, number):
    # TODO: this should probably be defined in a structural model, or profiles of a descriptive model
    if isinstance(item, Item) and umdform.pool_reports in item.format:
        if rootname.startswith('body-'):
            title = Literal('Body')
        else:
            title = Literal(f'Attachment {number - 1}')
    else:
        title = Literal(f'Page {number}')

    member = Page(title=title, number=Literal(number), member_of=item)

    # add the member to the item
    item.has_member.add(member)
    return member


def create_page(
        container: ContainerResource,
        parent: RDFResourceType,
        number: int,
        file_sources: Mapping[str, BinarySource] = None,
) -> ContainerResource:
    """Create a page with the given number, contained in container, as a pcdm:memberOf
    the parent RDF resource."""
    if file_sources is None:
        file_sources = {}
    logger.debug(f'Creating page {number} for {parent}')
    page_resource = container.create_child(
        resource_class=ContainerResource,
        slug=random_slug(),
        description=Page(title=f'Page {number}', number=number, member_of=parent),
    )
    files_container = page_resource.create_child(resource_class=ContainerResource, slug='f')
    with page_resource.describe(Page) as page:
        for filename, source in file_sources.items():
            file = create_file(container=files_container, source=source, parent=page)
            page.has_file.add(URIRef(file.url))
    page_resource.save()
    return page_resource


def create_file(
        container: ContainerResource,
        source: BinarySource,
        parent: RDFResourceType,
        slug: str = None,
) -> BinaryResource:
    """Create a single file"""
    if slug is None:
        slug = random_slug()
    title = basename(source.filename)
    with source.open() as stream:
        logger.debug(f'Creating file {source.filename} for {parent}')
        file_resource = container.create_child(
            resource_class=BinaryResource,
            slug=slug,
            description=PCDMFile(title=title, file_of=parent),
            data=stream,
            headers={
                'Content-Type': source.mimetype(),
                'Digest': source.digest(),
                'Content-Disposition': f'attachment; filename="{source.filename}"',
            },
        )
    file_resource.save()
    logger.debug(f'Created file: {file_resource.url} {title}')
    return file_resource


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
    def __init__(self, job_id, jobs_dir, ssh_private_key: str = None):
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

        self.ssh_private_key = ssh_private_key
        self.validation_reports = []

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
            repo: Repository,
            model: Type[Resource],
            access: URIRef = None,
            member_of: URIRef = None,
            container: str = None,
            binaries_location: str = None,
            extract_text_types: str = None,
            **kwargs,
    ) -> Generator[Any, None, Counter]:
        self.save_config({
            'model': model,
            'access': access,
            'member_of': member_of,
            'container': container,
            'binaries_location': binaries_location,
            'extract_text_types': extract_text_types,
        })

        return (yield from self.import_items(repo=repo, **kwargs))

    def resume(self, repo: Repository, **kwargs) -> Generator[Any, None, Counter]:
        if not self.dir_exists:
            raise RuntimeError(f'Cannot resume job "{self.id}": no such job directory: "{self.dir}"')

        logger.info(f'Resuming saved job {self.id}')
        try:
            # load stored config from the previous run of this job
            self.load_config()
        except FileNotFoundError:
            raise RuntimeError(f'Cannot resume job {self.id}: no config.yml found in {self.dir}')

        return (yield from self.import_items(repo=repo, **kwargs))

    def import_items(
            self,
            repo: Repository,
            limit: int = None,
            percentage=None,
            validate_only: bool = False,
            import_file: IO = None,
    ) -> Generator[Any, None, Counter]:
        start_time = datetime.now().timestamp()

        if percentage:
            logger.info(f'Loading {percentage}% of the total items')
        if validate_only:
            logger.info('Validation-only mode, skipping imports')

        # if an import file was provided, save that as the new CSV metadata file
        if import_file is not None:
            self.store_metadata_file(import_file)

        try:
            metadata = ImportSpreadsheet(metadata_filename=self.metadata_filename, model_class=self.model_class)
        except ModelClassNotFoundError as e:
            raise RuntimeError(f'Model class {e.model_name} not found') from e
        except JobError as e:
            raise RuntimeError(str(e)) from e

        if metadata.has_binaries and self.config.binaries_location is None:
            raise RuntimeError('Must specify --binaries-location if the metadata has a FILES column')

        count = Counter(
            total_items=metadata.total,
            rows=0,
            errors=0,
            initially_completed_items=len(self.completed_log),
            files=0,
            valid_items=0,
            invalid_items=0,
            created_items=0,
            updated_items=0,
            unchanged_items=0,
            skipped_items=0,
        )
        logger.info(f'Found {count["initially_completed_items"]} completed items')

        import_run = self.new_run().start()
        for row in metadata.rows(limit=limit, percentage=percentage, completed=self.completed_log):
            logger.debug(f'Row data: {row.data}')
            import_row = ImportRow(self, repo, row)

            # count the number of files referenced in this row
            count['files'] += len(row.filenames)

            # validate metadata and files
            validation = import_row.validate_item()
            if validation.ok:
                count['valid_items'] += 1
                logger.info(f'"{import_row}" is valid')
            else:
                # drop invalid items
                count['invalid_items'] += 1
                logger.warning(f'"{import_row}" is invalid, skipping')
                reasons = [f'{name} {result}' for name, result in validation.failures()]
                import_run.drop_invalid(
                    item=import_row.item,
                    line_reference=row.line_reference,
                    reason=f'Validation failures: {"; ".join(reasons)}'
                )
                continue

            if validate_only:
                # validation-only mode
                continue

            try:
                status = import_row.update_repo()
                self.complete(import_row.item, row.line_reference, status)
                if status == ImportedItemStatus.CREATED:
                    count['created_items'] += 1
                elif status == ImportedItemStatus.MODIFIED:
                    count['updated_items'] += 1
                elif status == ImportedItemStatus.UNCHANGED:
                    count['unchanged_items'] += 1
                    count['skipped_items'] += 1
                else:
                    raise RuntimeError(f'Unknown status "{status}" returned when importing "{import_row.item}"')
            except RuntimeError as e:
                count['items_with_errors'] += 1
                logger.error(f'{import_row.item} import failed: {e}')
                import_run.drop_failed(import_row.item, row.line_reference, reason=str(e))

            # update the status
            now = datetime.now().timestamp()
            yield {
                'time': {
                    'started': start_time,
                    'now': now,
                    'elapsed': now - start_time
                },
                'count': count,
            }

        return count

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

        previous_proxy: Optional[Proxy] = None
        for n, (rootname, filenames) in enumerate(file_groups.items(), 1):
            if create_pages:
                # create a member object for each rootname
                # delegate to the item model how to build the member object
                member = get_new_member(item, rootname, n)
                proxy = Proxy(proxy_for=member, proxy_in=item)
                if previous_proxy is not None:
                    previous_proxy.next = proxy
                    proxy.prev = previous_proxy
                else:
                    item.first = proxy
                # add the access class to the member resources
                if access is not None:
                    member.rdf_type.add(access)
                    proxy.rdf_type.add(access)
                file_parent = member
                previous_proxy = proxy
            else:
                # files will be added directly to the item
                file_parent = item

            # add the files to their parent object (either the item or a member)
            for filename in filenames:
                file = self.get_file(base_location, filename)
                count += 1
                # file_parent.add_file(file)
                if access is not None:
                    file.rdf_type.add(access)

        item.last = previous_proxy

        return count


class ImportRow:
    def __init__(self, job: ImportJob, repo: Repository, row: Row):
        self.job = job
        self.row = row
        self.repo = repo
        self.item = get_item_to_import(repo, row)

    def __str__(self):
        return str(self.item)

    def validate_item(self) -> ValidationResultsDict:
        """Validate the item for this import row, and check that all files
        listed are present.

        :returns: ValidationResult
        """
        try:
            results: ValidationResultsDict = self.item.validate()
        except ValidationError as e:
            raise RuntimeError(f'Unable to run validation: {e}') from e

        results['FILES'] = self.validate_files(self.row.filenames)
        results['ITEM_FILES'] = self.validate_files(self.row.item_filenames)

        return results

    def validate_files(self, filenames: Iterable[str]) -> ValidationResult:
        """Check that a file exists in the job's binaries location for each
        file name given.

        :returns: ValidationResult
        """
        missing_files = [
            name for name in filenames
            if not self.job.get_source(self.job.config.binaries_location, name).exists()
        ]
        if len(missing_files) == 0:
            return ValidationSuccess(
                prop=None,
                message='All files present',
            )
        else:
            return ValidationFailure(
                prop=None,
                message=f'Missing {len(missing_files)} files: {";".join(missing_files)}',
            )

    def update_repo(self) -> ImportedItemStatus:
        """
        Either creates a new item, updates an existing item, or does nothing
        to an existing item (if there are no changes).

        :returns: ImportedItemStatus
        """
        if self.item.uri.startswith('urn:uuid:'):
            # if an item is new, don't construct a SPARQL Update query
            # instead, just create and update normally
            # create new item in the repo
            logger.debug('Creating a new item')
            # add the access class
            if self.job.access is not None:
                self.item.rdf_type.append(self.job.access)
            # add the collection membership
            if self.job.member_of is not None:
                self.item.member_of = self.job.member_of

            if self.job.extract_text_types is not None:
                annotate_from_files(self.item, self.job.extract_text_types)

            container: ContainerResource = self.repo[self.job.config.container:ContainerResource]
            logger.debug(f"Creating resources in container: {container.path}")

            try:
                self.item.apply_changes()
                with self.repo.transaction():
                    # create the main resource
                    logger.debug(f'Creating main resource for "{self.item}"')
                    resource = container.create_child(
                        resource_class=ContainerResource,
                        description=self.item,
                    )
                    # hierarchical object: members container under the main resource
                    logger.debug(f'Creating members container for {resource.path}')
                    members_container = resource.create_child(resource_class=ContainerResource, slug='m')
                    proxies_container = resource.create_child(resource_class=ContainerResource, slug='x')
                    # add pages and files to those pages
                    if self.row.has_files:
                        file_groups = build_file_groups(self.row['FILES'])
                        with resource.describe(PCDMObject) as obj:
                            previous_proxy_resource = None
                            for n, (rootname, filenames) in enumerate(file_groups.items(), 1):
                                file_sources = {
                                    filename: self.job.get_source(self.job.config.binaries_location, filename)
                                    for filename in filenames
                                }
                                page_resource = create_page(
                                    container=members_container,
                                    parent=obj,
                                    number=n,
                                    file_sources=file_sources,
                                )
                                with page_resource.describe(Page) as page:
                                    proxy_resource = proxies_container.create_child(
                                        resource_class=ContainerResource,
                                        description=Proxy(
                                            proxy_for=page,
                                            proxy_in=obj,
                                            title=page.title.value,
                                        ),
                                        slug=random_slug(),
                                    )
                                    with proxy_resource.describe(Proxy) as proxy:
                                        if previous_proxy_resource is None:
                                            # first page in sequence
                                            obj.first = proxy
                                            obj.last = proxy
                                        else:
                                            with previous_proxy_resource.describe(Proxy) as previous_proxy:
                                                previous_proxy.next = proxy
                                                proxy.prev = previous_proxy
                                                previous_proxy.save()
                                                obj.last = proxy
                                    proxy_resource.save()
                                    previous_proxy_resource = proxy_resource
                                    obj.has_member.add(page)
                    """
                    if self.row.has_item_files:
                        item_files_container = resource.create_child(resource_class=ContainerResource, slug='f')
                        for rootname, filenames in self.row.item_filenames:
                            for filename in filenames:
                                source = self.job.get_source(self.job.config.binaries_location, filename)
                                file = create_file(
                                    container=item_files_container,
                                    source=source,
                                    parent=resource.description,
                                )
                                resource.description.has_file.add(URIRef(file.url))
                    """
                    resource.save()
            except Exception as e:
                raise RuntimeError(f'Creating item failed: {e}') from e

            logger.info(f'Created {resource.url}')
            return ImportedItemStatus.CREATED

        elif self.item.has_changes:
            # construct the SPARQL Update query if there are any deletions or insertions
            # then do a PATCH update of an existing item
            logger.info(f'Sending update for {self.item}')
            sparql_update = self.repo.client.build_sparql_update(self.item.deletes, self.item.inserts)
            logger.debug(sparql_update)
            try:
                self.item.patch(self.repo.client, sparql_update)
            except ClientError as e:
                raise RuntimeError(f'Updating item failed: {e}') from e

            return ImportedItemStatus.MODIFIED

        else:
            logger.info(f'No changes found for "{self.item}" ({self.item.uri}); skipping')
            return ImportedItemStatus.UNCHANGED


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
