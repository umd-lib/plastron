import logging
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from shutil import copyfileobj
from typing import Optional, Any, IO, Generator, Iterable

from bs4 import BeautifulSoup
from rdflib import URIRef

from plastron.client import ClientError
from plastron.context import PlastronContext
from plastron.files import BinarySource, ZipFileSource, RemoteFileSource, HTTPFileSource, LocalFileSource
from plastron.handles import HandleInfo
from plastron.jobs import JobError, JobConfig, Job, ItemLog
from plastron.jobs.importjob.spreadsheet import MetadataSpreadsheet, InvalidRow, Row, MetadataError
from plastron.models import get_model_from_name, ModelClassNotFoundError
from plastron.models.annotations import FullTextAnnotation, TextualBody
from plastron.namespaces import sc
from plastron.rdfmapping.validation import ValidationResultsDict, ValidationResult, ValidationSuccess, ValidationFailure
from plastron.repo import RepositoryError, ContainerResource
from plastron.repo.pcdm import PCDMObjectResource
from plastron.repo.publish import PublishableResource
from plastron.utils import datetimestamp
from plastron.validation import ValidationError

logger = logging.getLogger(__name__)
DROPPED_INVALID_FIELDNAMES = ['id', 'timestamp', 'title', 'uri', 'reason']
DROPPED_FAILED_FIELDNAMES = ['id', 'timestamp', 'title', 'uri', 'reason']


class ImportedItemStatus(Enum):
    CREATED = 'created'
    MODIFIED = 'modified'
    UNCHANGED = 'unchanged'


@dataclass
class ImportConfig(JobConfig):
    model: Optional[str] = None
    access: Optional[str] = None
    member_of: Optional[str] = None
    container: Optional[str] = None
    binaries_location: Optional[str] = None
    extract_text_types: Optional[str] = None


def get_loggable_uri(item):
    uri = getattr(item, 'uri', '')
    if uri.startswith('urn:uuid:'):
        # if the URI is a placeholder urn:uuid:, omit it from the log since
        # it has no meaning outside this particular running import job
        return ''
    return uri


class ImportRun:
    """
    A single run of an import job. Records the logs of invalid and failed items (if any).
    """

    def __init__(self, job: 'ImportJob'):
        self.job = job
        self.dir = None
        self.timestamp = None
        self._invalid_items = None
        self._failed_items = None
        self.start_time = None
        self.count = None
        self.state = None

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

    def progress_message(self, n: int, **kwargs) -> dict[str, Any]:
        now = datetime.now().timestamp()
        return {
            'time': {
                'started': self.start_time,
                'now': now,
                'elapsed': now - self.start_time
            },
            'count': self.count,
            'state': self.state,
            'progress': int(n / self.count['total_items'] * 100),
            **kwargs,
        }

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(
            self,
            context: PlastronContext,
            limit: int = None,
            percentage: int = None,
            validate_only: bool = False,
            import_file: IO = None,
            publish: bool = False,
    ) -> Generator[dict[str, Any], None, dict[str, Any]]:
        """Execute this import run. Returns a generator that yields a dictionary of
        current status after each item. The generator also returns a final status
        dictionary after the run has completed. This value can be captured using
        a delegating generator and Python's `yield from` syntax:

        ```python
        import_run = ImportRun(job)
        result = None

        # this is the delegated generator function
        def generator(repo):
            result = yield from import_run.run(repo)

        for status in generator(repo):
            print(status['count'], 'items completed')

        print('job status', result['type'])
        ```
        """
        if self.dir is not None:
            raise RuntimeError('Run completed, cannot start again')
        self.timestamp = datetimestamp()
        self.dir = self.job.dir / self.timestamp
        self.dir.mkdir(parents=True, exist_ok=True)
        self.start_time = datetime.now().timestamp()

        if percentage:
            logger.info(f'Loading {percentage}% of the total items')
        if validate_only:
            logger.info('Validation-only mode, skipping imports')
        if publish:
            logger.info('Publishing all imported items')

        # if an import file was provided, save that as the new CSV metadata file
        if import_file is not None:
            self.job.store_metadata_file(import_file)

        try:
            metadata = self.job.get_metadata()
        except ModelClassNotFoundError as e:
            raise RuntimeError(f'Model class {e.model_name} not found') from e
        except JobError as e:
            raise RuntimeError(str(e)) from e

        self.count = Counter(
            total_items=metadata.total,
            rows=0,
            errors=0,
            initially_completed_items=len(self.job.completed_log),
            files=0,
            valid_items=0,
            invalid_items=0,
            created_items=0,
            updated_items=0,
            unchanged_items=0,
            skipped_items=0,
        )
        logger.info(f'Found {self.count["initially_completed_items"]} completed items')
        if self.count['initially_completed_items'] > 0:
            logger.debug(f'Completed item identifiers: {self.job.completed_log.item_keys}')

        self.state = 'validate_in_progress' if validate_only else 'import_in_progress'
        yield self.progress_message(0)
        for n, row in enumerate(metadata.rows(limit=limit, percentage=percentage, completed=self.job.completed_log), 1):
            if isinstance(row, InvalidRow):
                self.drop_invalid(item=None, line_reference=row.line_reference, reason=row.reason)
                self.count['invalid_items'] += 1
                yield self.progress_message(n)
                continue

            logger.debug(f'Row data: {row.data}')
            import_row = ImportRow(self.job, context, row, validate_only, publish)

            # count the number of files referenced in this row
            self.count['files'] += len(row.filenames)

            # validate metadata and files
            try:
                validation = import_row.validate_item()
            except RuntimeError as e:
                self.count['errors'] += 1
                logger.warning(f'"{import_row}" caused an error, skipping')
                self.drop_failed(
                    item=import_row.item,
                    line_reference=row.line_reference,
                    reason=str(e),
                )
                yield self.progress_message(n)
                continue

            if validation.ok:
                self.count['valid_items'] += 1
                logger.info(f'"{import_row}" is valid')
            else:
                # drop invalid items
                self.count['invalid_items'] += 1
                logger.warning(f'"{import_row}" is invalid, skipping')
                reasons = [f'{name} {result}' for name, result in validation.failures()]
                self.drop_invalid(
                    item=import_row.item,
                    line_reference=row.line_reference,
                    reason=f'Validation failures: {"; ".join(reasons)}'
                )
                yield self.progress_message(n)
                continue

            if validate_only:
                # validation-only mode
                yield self.progress_message(n)
                continue

            try:
                status = import_row.update_repo()
                self.complete(import_row, status)
                if status == ImportedItemStatus.CREATED:
                    self.count['created_items'] += 1
                elif status == ImportedItemStatus.MODIFIED:
                    self.count['updated_items'] += 1
                elif status == ImportedItemStatus.UNCHANGED:
                    self.count['unchanged_items'] += 1
                    self.count['skipped_items'] += 1
                else:
                    raise RuntimeError(f'Unknown status "{status}" returned when importing "{import_row.item}"')
            except JobError as e:
                self.count['items_with_errors'] += 1
                logger.error(f'{import_row} import failed: {e}')
                self.drop_failed(import_row.item, row.line_reference, reason=str(e))

            # update the status
            yield self.progress_message(n)

        if validate_only:
            # validate phase
            if self.count['invalid_items'] == 0 and self.count['errors'] == 0:
                self.state = 'validate_success'
            else:
                self.state = 'validate_failed'
        else:
            # import phase
            if len(self.job.completed_log) == self.count['total_items']:
                self.state = 'import_complete'
            else:
                self.state = 'import_incomplete'

        return self.progress_message(
            n=self.count['total_items'],
            type=self.state,
            validation=self.job.validation_reports,
        )

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
            'uri': get_loggable_uri(item),
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
            f'Dropping invalid row {line_reference} from import job "{self.job}" run {self.timestamp}: {reason}'
        )
        self.invalid_items.append({
            'id': getattr(item, 'identifier', line_reference),
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(item, 'title', ''),
            'uri': get_loggable_uri(item),
            'reason': reason
        })

    def complete(self, row: 'ImportRow', status: ImportedItemStatus):
        """
        Delegates to the `ImportJob.complete()` method.
        """
        self.job.complete(row, status)


class ImportJob(Job):
    run_class = ImportRun
    config_class = ImportConfig

    def __init__(self, job_id: str, job_dir: Path, ssh_private_key: str = None):
        super().__init__(job_id=job_id, job_dir=job_dir)

        self._model_class = None
        self.ssh_private_key = ssh_private_key
        self.validation_reports = []

    @property
    def metadata_file(self) -> Path:
        return self.dir / 'source.csv'

    @property
    def model_class(self):
        if self._model_class is None:
            self._model_class = get_model_from_name(self.config.model)
        return self._model_class

    def store_metadata_file(self, input_file: IO):
        with self.metadata_file.open(mode='w') as file:
            copyfileobj(input_file, file)
            logger.debug(f"Copied input file {getattr(input_file, 'name', '<>')} to {file.name}")

    def complete(self, row: 'ImportRow', status: ImportedItemStatus):
        # write to the completed item log
        self.completed_log.append({
            'id': row.identifier,
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(row.item, 'title', ''),
            'uri': getattr(row.item, 'uri', ''),
            'status': status.value
        })

    def get_metadata(self) -> MetadataSpreadsheet:
        try:
            return MetadataSpreadsheet(metadata_filename=self.metadata_file, model_class=self.model_class)
        except MetadataError as e:
            raise JobError(job=self) from e

    def run(
            self,
            context: PlastronContext,
            limit: int = None,
            percentage: int = None,
            validate_only: bool = False,
            import_file: IO = None,
            publish: bool = False,
    ) -> Generator[dict[str, Any], None, dict[str, Any]]:
        run = self.new_run()
        return run(
            context=context,
            limit=limit,
            percentage=percentage,
            validate_only=validate_only,
            import_file=import_file,
            publish=publish,
        )

    @property
    def access(self) -> Optional[URIRef]:
        if self.config.access is not None:
            return URIRef(self.config.access)
        else:
            return None

    @property
    def member_of(self) -> Optional[URIRef]:
        if self.config.member_of is not None:
            return URIRef(self.config.member_of)
        else:
            return None

    @property
    def extract_text_types(self) -> list[str]:
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


class PublishableObjectResource(PCDMObjectResource, PublishableResource):
    pass


class ImportRow:
    def __init__(
            self,
            job: ImportJob,
            context: PlastronContext,
            row: Row,
            validate_only: bool = False,
            publish: bool = None,
    ):
        self.job = job
        self.row = row
        self.context = context
        self.item = row.get_object(context.repo, read_from_repo=not validate_only)
        if publish is not None:
            self._publish = publish

    def __str__(self):
        return str(self.row.line_reference)

    @property
    def identifier(self) -> str:
        return self.row.identifier

    def validate_item(self) -> ValidationResultsDict:
        """Validate the item for this import row, and check that all files
        listed are present."""
        try:
            results: ValidationResultsDict = self.item.validate()
        except ValidationError as e:
            raise RuntimeError(f'Unable to run validation: {e}') from e

        # binaries_location is only required if there is a value in the
        # "FILES" or "ITEM_FILES" column.
        # This is to enable CSVs originally generated from an Archelon
        # export job to be used as an import CSV file
        if (self.row.has_files or self.row.has_item_files) and not self.job.config.binaries_location:
            raise RuntimeError('Must specify --binaries-location if the metadata has a FILES and/or ITEM_FILES column')

        results['FILES'] = self.validate_files(self.row.filenames)
        results['ITEM_FILES'] = self.validate_files(f.name for f in self.row.item_files)

        return results

    def validate_files(self, filenames: Iterable[str]) -> ValidationResult:
        """Check that a file exists in the job's binaries location for each
        file name given."""
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
        """Either creates a new item, updates an existing item, or does nothing
        to an existing item (if there are no changes)."""
        if self.item.uri.startswith('urn:uuid:'):
            resource = self.create_resource()
            logger.info(f'Created {resource.url}')
            return ImportedItemStatus.CREATED

        elif self.item.has_changes:
            # construct the SPARQL Update query if there are any deletions or insertions
            # then do a PATCH update of an existing item
            try:
                resource: PublishableObjectResource = self.context.repo[self.item.uri:PublishableObjectResource].read()
                resource.attach_description(self.item)
                resource.update()
                # publish this resource, if requested
                if self._publish:
                    self.publish(resource)
            except RepositoryError as e:
                raise JobError(f'Updating item failed: {e}') from e

            logger.info(f'Updated {resource.url}')
            return ImportedItemStatus.MODIFIED

        else:
            logger.info(f'No changes found for "{self.item}" ({self.item.uri}); skipping')
            return ImportedItemStatus.UNCHANGED

    def publish(self, resource: PublishableObjectResource) -> HandleInfo:
        return resource.publish(
            handle_client=self.context.handle_client,
            public_url=self.context.get_public_url(resource),
        )

    def create_resource(self) -> PublishableObjectResource:
        """Create a new item in the repository."""

        # if an item is new, don't construct a SPARQL Update query
        # instead, just create and update normally
        logger.debug(f'Creating a new item for "{self.row.line_reference}"')
        # add the access class
        logger.debug(f'Access class: {self.job.access}')
        if self.job.access is not None:
            self.item.rdf_type.add(self.job.access)
        # add the collection membership
        logger.debug(f'Member of: {self.job.member_of}')
        if self.job.member_of is not None:
            self.item.member_of = self.job.member_of

        if self.job.extract_text_types is not None:
            annotate_from_files(self.item, self.job.extract_text_types)

        logger.debug(f"Creating resources in container: {self.job.config.container}")
        logger.debug(f'Repo: {self.context.repo}')
        container: ContainerResource = self.context.repo[self.job.config.container:ContainerResource]

        try:
            with self.context.repo.transaction():
                # create the main resource
                logger.debug(f'Creating main resource for "{self.item}"')
                resource = container.create_child(
                    resource_class=PublishableObjectResource,
                    description=self.item,
                )
                # add pages and files to those pages
                if self.row.has_files:
                    # update the file_groups to have a source
                    for file_group in self.row.file_groups.values():
                        for file in file_group.files:
                            file.source = self.job.get_source(self.job.config.binaries_location, file.name)
                    resource.create_page_sequence(self.row.file_groups)

                # item-level files
                if self.row.has_item_files:
                    for file in self.row.item_files:
                        source = self.job.get_source(self.job.config.binaries_location, file.name)
                        resource.create_file(source=source, rdf_types=file.rdf_types)

                # publish this resource, if requested
                if self._publish:
                    self.publish(resource)

        except ClientError as e:
            raise JobError(self.job, f'Creating item failed: {e}', e.response.text) from e
        else:
            return resource


def annotate_from_files(item, mime_types):
    for member in item.has_member.objects:
        # extract text from HTML files
        for file in filter(lambda f: str(f.mimetype) in mime_types, member.has_file.objects):
            if str(file.mimetype) == 'text/html':
                # get text from HTML
                with file.source as stream:
                    text = BeautifulSoup(b''.join(stream), features='lxml').get_text()
            else:
                logger.warning(f'Extracting text from {file.mimetype} is not supported')
                continue

            annotation = FullTextAnnotation(
                target=member,
                body=TextualBody(value=text, content_type='text/plain'),
                motivation=sc.painting,
                derived_from=file
            )
            # don't embed full resources
            annotation.props['target'].is_embedded = False

            member.annotations.append(annotation)
