import copy
import csv
import logging
import os
from argparse import FileType, ArgumentTypeError
from os.path import basename
from typing import TextIO

from rdflib import URIRef

from plastron.cli.commands import BaseCommand
from plastron.client import Client, ClientError
from plastron.core.exceptions import ConfigError, FailureException
from plastron.core.util import datetimestamp, strtobool
from plastron.files import HTTPFileSource, LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.jobs import ImportJob, ImportedItemStatus, ModelClassNotFoundError, ImportOperation
from plastron.jobs.utils import build_file_groups, annotate_from_files
from plastron.models import get_model_class
from plastron.namespaces import get_manager
from plastron.rdf import uri_or_curie
from plastron.rdf.pcdm import File, PreservationMasterFile
from plastron.repo import Repository

nsm = get_manager()
logger = logging.getLogger(__name__)


# custom argument type for percentage loads
def percentile(n):
    p = int(n)
    if not p > 0 and p < 100:
        raise ArgumentTypeError("Percent param must be 1-99")
    return p


def write_model_template(model_name: str, template_file: TextIO):
    try:
        model_class = get_model_class(model_name)
    except ModelClassNotFoundError as e:
        raise RuntimeError(f'Cannot find model class named {model_name}') from e
    if not hasattr(model_class, 'HEADER_MAP'):
        raise RuntimeError(f'{model_class.__name__} has no HEADER_MAP, cannot create template')

    logger.info(f'Writing template for the {model_class.__name__} model to {template_file.name}')
    writer = csv.writer(template_file)
    writer.writerow(list(model_class.HEADER_MAP.values()) + ['FILES', 'ITEM_FILES'])


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='import',
        description='Import data to the repository'
    )
    parser.add_argument(
        '-m', '--model',
        help='data model to use',
        action='store'
    )
    parser.add_argument(
        '-l', '--limit',
        help='limit the number of rows to read from the import file',
        type=int,
        action='store'
    )
    parser.add_argument(
        '-%', '--percent',
        help=(
            'select an evenly spaced subset of items to import; '
            'the size of this set will be as close as possible '
            'to the specified percentage of the total items'
        ),
        type=percentile,
        dest='percentage',
        action='store'
    )
    parser.add_argument(
        '--validate-only',
        help='only validate, do not do the actual import',
        action='store_true'
    )
    parser.add_argument(
        '--make-template',
        help='create a CSV template for the given model',
        dest='template_file',
        metavar='FILENAME',
        type=FileType('w'),
        action='store'
    )
    parser.add_argument(
        '--access',
        help='URI or CURIE of the access class to apply to new items',
        type=uri_or_curie,
        metavar='URI|CURIE',
        action='store'
    )
    parser.add_argument(
        '--member-of',
        help='URI of the object that new items are PCDM members of',
        metavar='URI',
        action='store'
    )
    parser.add_argument(
        '--binaries-location',
        help=(
            'where to find binaries; either a path to a directory, '
            'a "zip:<path to zipfile>" URI, an SFTP URI in the form '
            '"sftp://<user>@<host>/<path to dir>", or a URI in the '
            'form "zip+sftp://<user>@<host>/<path to zipfile>"'
        ),
        metavar='LOCATION',
        action='store'
    )
    parser.add_argument(
        '--container',
        help=(
            'parent container for new items; defaults to the RELPATH '
            'in the repo configuration file'
        ),
        metavar='PATH',
        action='store'
    )
    parser.add_argument(
        '--job-id',
        help='unique identifier for this job; defaults to "import-{timestamp}"',
        action='store'
    )
    parser.add_argument(
        '--resume',
        help='resume a job that has been started; requires --job-id {id} to be present',
        action='store_true'
    )
    parser.add_argument(
        '--extract-text-from', '-x',
        help=(
            'extract text from binaries of the given MIME types, '
            'and add as annotations'
        ),
        dest='extract_text_types',
        metavar='MIME_TYPES',
        action='store'
    )
    parser.add_argument(
        'import_file', nargs='?',
        help='name of the file to import from',
        type=FileType('r', encoding='utf-8-sig'),
        action='store'
    )
    parser.set_defaults(cmd_name='import')


class Command(BaseCommand):
    def __init__(self, config=None):
        super().__init__(config=config)
        self.result = None
        self.ssh_private_key = self.config.get('SSH_PRIVATE_KEY')
        self.jobs_dir = self.config.get('JOBS_DIR', 'jobs')

    def __call__(self, *args, **kwargs):
        for _ in self.execute(*args, **kwargs):
            pass

    def repo_config(self, repo_config, args=None):
        """
        Returns a deep copy of the provided repo_config, updated with
        layout structure and relpath information from the args
        (if provided). If no args are provided, just run the base command
        repo_config() method.
        """
        if args is None:
            return super().repo_config(repo_config, args)

        result_config = copy.deepcopy(repo_config)

        if args.structure:
            result_config['STRUCTURE'] = args.structure

        if args.relpath:
            result_config['RELPATH'] = args.relpath

        return result_config

    def get_source(self, base_location, path):
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

    def get_file(self, base_location, filename):
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

    def add_files(self, item, file_groups, base_location, access=None, create_pages=True):
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
            raise ConfigError('Must specify a binaries-location')

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

    @staticmethod
    def create_import_job(job_id, jobs_dir):
        """
        Returns an ImportJob with the given parameters

        :param job_id: the job id for the import job
        :param jobs_dir: the base directory where job information is stored
        :return: An ImportJob with the given parameters
        """
        return ImportJob(job_id, jobs_dir=jobs_dir)

    def execute(self, client: Client, args):
        """
        Performs the import

        :param client: the repository configuration
        :param args: the command-line arguments
        """
        if hasattr(args, 'template_file') and args.template_file is not None:
            write_model_template(args.model, args.template_file)
            return

        if not args.resume:
            if args.import_file is None:
                raise RuntimeError('An import file is required unless resuming an existing job')

            if args.model is None:
                raise RuntimeError('A model is required unless resuming an existing job')

        if args.resume and args.job_id is None:
            raise RuntimeError('Resuming a job requires a job id')

        if args.job_id is None:
            # TODO: generate a more unique id? add in user and hostname?
            args.job_id = f"import-{datetimestamp()}"

        repo = Repository(client=client)
        operation = ImportOperation(args.job_id, self.jobs_dir, repo=repo)
        if args.resume:
            metadata = yield from operation.resume()
        else:
            metadata = yield from operation.start(
                import_file=args.import_file,
                model=args.model,
                access=args.access,
                member_of=args.member_of,
                container=args.container,
                binaries_location=args.binaries_location,
            )

        """ TODO: statistics object to return
        logger.info(f'Skipped {metadata.skipped} items')
        logger.info(f'Completed {len(job.completed_log) - initial_completed_item_count} items')
        logger.info(f'Dropped {len(import_run.invalid_items)} invalid items')
        logger.info(f'Dropped {len(import_run.failed_items)} failed items')

        logger.info(f"Found {metadata.valid} valid items")
        logger.info(f"Found {metadata.invalid} invalid items")
        logger.info(f"Found {metadata.errors} errors")
        if not args.validate_only:
            logger.info(f"{metadata.unchanged} of {metadata.total} items remained unchanged")
            logger.info(f"Created {metadata.created} of {metadata.total} items")
            logger.info(f"Updated {metadata.updated} of {metadata.total} items")
        """

        if args.validate_only:
            # validate phase
            if metadata.invalid == 0:
                result_type = 'validate_success'
            else:
                result_type = 'validate_failed'
        else:
            # import phase
            if len(operation.job.completed_log) == metadata.total:
                result_type = 'import_complete'
            else:
                result_type = 'import_incomplete'

        self.result = {
            'type': result_type,
            'validation': metadata.validation_reports,
            'count': metadata.stats()
        }

    def update_repo(self, args, job, client: Client, metadata, row, repo_changeset, created_uris, updated_uris):
        """
        Updates the repository with the given RepoChangeSet

        :param args: the arguments from the command-line
        :param job: The ImportJob
        :param client: the repository configuration
        :param metadata: A plastron.jobs.MetadataRows object representing the
                          CSV file being imported
        :param row: A single plastron.jobs.Row object representing the row
                     being imported
        :param repo_changeset: The RepoChangeSet object describing the changes
                                 to make to the repository.
        :param created_uris: Accumulator storing a list of created URIS. This
                              variable is MODIFIED by this method.
        :param updated_uris: Accumulator storing a list of updated URIS. This
                              variable is MODIFIED by this method.
        """
        item = repo_changeset.item

        if not item.created:
            # if an item is new, don't construct a SPARQL Update query
            # instead, just create and update normally
            # create new item in the repo
            logger.debug('Creating a new item')
            # add the access class
            if job.access is not None:
                item.rdf_type.append(URIRef(job.access))
            # add the collection membership
            if job.member_of is not None:
                item.member_of = URIRef(job.member_of)

            if row.has_files:
                create_pages = bool(strtobool(row.get('CREATE_PAGES', 'True')))
                logger.debug('Adding pages and files to new item')
                self.add_files(
                    item,
                    build_file_groups(row['FILES']),
                    base_location=job.binaries_location,
                    access=job.access,
                    create_pages=create_pages
                )

            if row.has_item_files:
                self.add_files(
                    item,
                    build_file_groups(row['ITEM_FILES']),
                    base_location=job.binaries_location,
                    access=job.access,
                    create_pages=False
                )

            if args.extract_text_types is not None:
                annotate_from_files(item, args.extract_text_types.split(','))

            logger.debug(f"Creating resources in container: {job.container}")

            try:
                with client.transaction() as txn_client:
                    item.create(txn_client, container_path=job.container)
                    item.update(txn_client)
                    txn_client.commit()
            except Exception as e:
                raise FailureException(f'Creating item failed: {e}') from e

            job.complete(item, row.line_reference, ImportedItemStatus.CREATED)
            metadata.created += 1
            created_uris.append(item.uri)

        elif repo_changeset:
            # construct the SPARQL Update query if there are any deletions or insertions
            # then do a PATCH update of an existing item
            logger.info(f'Sending update for {item}')
            sparql_update = repo_changeset.build_sparql_update(client)
            logger.debug(sparql_update)
            try:
                item.patch(client, sparql_update)
            except ClientError as e:
                raise FailureException(f'Updating item failed: {e}') from e

            job.complete(item, row.line_reference, ImportedItemStatus.MODIFIED)
            metadata.updated += 1
            updated_uris.append(item.uri)

        else:
            job.complete(item, row.line_reference, ImportedItemStatus.UNCHANGED)
            metadata.unchanged += 1
            logger.info(f'No changes found for "{item}" ({row.uri}); skipping')
            metadata.skipped += 1
