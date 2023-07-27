import copy
import csv
import io
import logging
import os
import re
from argparse import FileType, Namespace, ArgumentTypeError
from collections import OrderedDict, defaultdict
from datetime import datetime
from os.path import basename, splitext
from uuid import uuid4

from bs4 import BeautifulSoup
from rdflib import Graph, Literal, URIRef

from plastron.client import Client
from plastron.commands import BaseCommand
from plastron.core.exceptions import ConfigError, FailureException, RESTAPIException
from plastron.files import HTTPFileSource, LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.jobs import ImportJob, ImportedItemStatus, JobError, ModelClassNotFoundError, build_lookup_index
from plastron.namespaces import get_manager, prov, sc
from plastron.rdf import rdf, uri_or_curie
from plastron.rdf.oa import Annotation, TextualBody
from plastron.rdf.pcdm import File, PreservationMasterFile
from plastron.rdf.rdf import RDFDataProperty
from plastron.core.util import datetimestamp, strtobool
from plastron.validation import ValidationError, validate

nsm = get_manager()
logger = logging.getLogger(__name__)


# custom argument type for percentage loads
def percentile(n):
    p = int(n)
    if not p > 0 and p < 100:
        raise ArgumentTypeError("Percent param must be 1-99")
    return p


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


def get_property_type(model_class: rdf.Resource, attrs):
    if '.' in attrs:
        first, rest = attrs.split('.', 2)
        return get_property_type(model_class.name_to_prop[first].obj_class, rest)
    else:
        return model_class.name_to_prop[attrs]


def build_file_groups(filenames_string):
    file_groups = OrderedDict()
    if filenames_string.strip() == '':
        return file_groups
    for filename in filenames_string.split(';'):
        root, ext = splitext(basename(filename))
        if root not in file_groups:
            file_groups[root] = []
        file_groups[root].append(filename)
    logger.debug(f'Found {len(file_groups.keys())} unique file basename(s)')
    return file_groups


def not_empty(value):
    return value is not None and value != ''


def split_escaped(string: str, separator: str = '|'):
    # uses a negative look-behind to only split on separator characters
    # that are NOT preceded by an escape character (the backslash)
    pattern = re.compile(r'(?<!\\)' + re.escape(separator))
    values = pattern.split(string)
    # remove the escape character
    return [re.sub(r'\\(.)', r'\1', v) for v in values]


def parse_value_string(value_string, column, prop_type):
    # filter out empty strings, so we don't get spurious empty values in the properties
    for value in filter(not_empty, split_escaped(value_string, separator='|')):
        if issubclass(prop_type, RDFDataProperty):
            # default to the property's defined datatype
            # if it was not specified in the column header
            yield Literal(value, lang=column['lang_code'], datatype=column.get('datatype', prop_type.datatype))
        else:
            yield URIRef(value)


def annotate_from_files(item, mime_types):
    for member in item.members:
        # extract text from HTML files
        for file in filter(lambda f: str(f.mimetype) in mime_types, member.files):
            if str(file.mimetype) == 'text/html':
                # get text from HTML
                with file.source as stream:
                    text = BeautifulSoup(io.BytesIO(b''.join(stream)), features='lxml').get_text()
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


def create_repo_changeset(repo, metadata, row, validate_only=False):
    """
    Returns a RepoChangeset of the changes to make to the repository

    :param repo: the repository configuration
    :param metadata: A plastron.jobs.MetadataRows object representing the
                      CSV file for the import
    :param row: A single plastron.jobs.Row object representing the row
                 to import
    :param validate_only: If true, will not fetch existing object from the
                repository.
    :return: A RepoChangeSet encapsulating the changes to make to the
            repository.
    """
    if validate_only:
        # create an empty object to validate without fetching from the repo
        item = metadata.model_class(uri=row.uri)
    else:
        if row.uri is not None:
            # read the object from the repo
            item = metadata.model_class.from_repository(repo, row.uri, include_server_managed=False)
        else:
            # no URI in the CSV means we will create a new object
            logger.info(f'No URI found for {row.line_reference}; will create new resource')
            # create a new object (will create in the repo later)
            item = metadata.model_class()

    # track new embedded objects that are added to the graph
    # so we can ensure that they have at least one statement
    # where they appear as the subject
    new_objects = defaultdict(Graph)

    delete_graph = Graph()
    insert_graph = Graph()

    # build the lookup index to map hash URI objects
    # to their correct positional locations
    row_index = build_lookup_index(item, row.index_string)

    for attrs, columns in metadata.fields.items():
        prop_type = get_property_type(item.__class__, attrs)
        if '.' not in attrs:
            # simple, non-embedded values
            # attrs is the entire property name
            new_values = []
            for column in columns:
                header = column['header']
                new_values.extend(parse_value_string(row[header], column, prop_type))

            # construct a SPARQL update by diffing for deletions and insertions
            # update the property and get the sets of values deleted and inserted
            prop = getattr(item, attrs)
            deleted_values, inserted_values = prop.update(new_values)

            for deleted_value in deleted_values:
                delete_graph.add((item.uri, prop.uri, prop.get_term(deleted_value)))
            for inserted_value in inserted_values:
                insert_graph.add((item.uri, prop.uri, prop.get_term(inserted_value)))

        else:
            # complex, embedded values

            # if the first portion of the dotted attr notation is a key in the index,
            # then this column has a different subject than the main uri
            # correlate positions and urirefs
            # XXX: for now, assuming only 2 levels of chaining
            first_attr, next_attr = attrs.split('.', 2)
            new_values = defaultdict(list)
            for column in columns:
                header = column['header']
                for i, value_string in enumerate(row[header].split(';')):
                    new_values[i].extend(parse_value_string(value_string, column, prop_type))

            if first_attr in row_index:
                # existing embedded object
                for i, values in new_values.items():
                    # get the embedded object
                    obj = row_index[first_attr][i]
                    prop = getattr(obj, next_attr)
                    deleted_values, inserted_values = prop.update(values)

                    for deleted_value in deleted_values:
                        delete_graph.add((item.uri, prop.uri, prop.get_term(deleted_value)))
                    for inserted_value in inserted_values:
                        insert_graph.add((item.uri, prop.uri, prop.get_term(inserted_value)))
            else:
                # create new embedded objects (a.k.a hash resources) that are not in the index
                first_prop_type = item.name_to_prop[first_attr]
                for i, values in new_values.items():
                    # we can assume that for any properties with dotted notation,
                    # all attributes except for the last one are object properties
                    if first_prop_type.obj_class is not None:
                        # create a new object
                        # TODO: remove hardcoded UUID fragment minting
                        obj = first_prop_type.obj_class(uri=f'{item.uri}#{uuid4()}')
                        # add the new object to the index
                        row_index[first_attr][i] = obj
                        setattr(obj, next_attr, values)
                        next_attr_prop = obj.name_to_prop[next_attr]
                        for value in values:
                            new_objects[(first_attr, obj)].add((obj.uri, next_attr_prop.uri, value))

    # add new embedded objects to the insert graph
    for (attr, obj), graph in new_objects.items():
        # add that object to the main item
        getattr(item, attr).append(obj)
        # add to the insert graph
        insert_graph.add((item.uri, item.name_to_prop[attr].uri, obj.uri))
        insert_graph += graph

    # do a pass to remove statements that are both deleted and then re-inserted
    for statement in delete_graph:
        if statement in insert_graph:
            delete_graph.remove(statement)
            insert_graph.remove(statement)

    return RepoChangeset(item, insert_graph, delete_graph)


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
    def parse_message(message):
        access = message.args.get('access')
        message.body = message.body.encode('utf-8').decode('utf-8-sig')
        if access is not None:
            try:
                access_uri = uri_or_curie(access)
            except ArgumentTypeError as e:
                raise FailureException(f'PlastronArg-access {e}')
        else:
            access_uri = None
        return Namespace(
            model=message.args.get('model'),
            limit=message.args.get('limit', None),
            percentage=message.args.get('percent', None),
            validate_only=message.args.get('validate-only', False),
            resume=message.args.get('resume', False),
            import_file=io.StringIO(message.body),
            template_file=None,
            access=access_uri,
            member_of=message.args.get('member-of'),
            binaries_location=message.args.get('binaries-location'),
            container=message.args.get('container', None),
            extract_text_types=message.args.get('extract-text', None),
            job_id=message.job_id,
            structure=message.args.get('structure', None),
            relpath=message.args.get('relpath', None)
        )

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
        start_time = datetime.now().timestamp()

        if args.resume and args.job_id is None:
            raise FailureException('Resuming a job requires a job id')

        if args.job_id is None:
            # TODO: generate a more unique id? add in user and hostname?
            args.job_id = f"import-{datetimestamp()}"

        job: ImportJob = Command.create_import_job(args.job_id, jobs_dir=self.jobs_dir)
        logger.debug(f'Job directory is {job.dir}')

        if args.resume and not job.dir_exists:
            raise FailureException(f'Cannot resume job {job.id}: no such job directory found in {self.jobs_dir}')

        # load or create config
        if args.resume:
            logger.info(f'Resuming saved job {job.id}')
            # load stored config from the previous run of this job
            try:
                job.load_config()
            except FileNotFoundError:
                raise FailureException(f'Cannot resume job {job.id}: no config.yml found in {job.dir}')
        else:
            if args.model is None:
                raise FailureException('A model is required unless resuming an existing job')
            job.save_config({
                'model': args.model,
                'access': args.access,
                'member_of': args.member_of,
                # Use "repo.relpath" as default for "container",
                # but allow it to be overridden by args
                'container': args.container or client.repo.relpath,
                'binaries_location': args.binaries_location
            })

        if args.template_file is not None:
            if not hasattr(job.model_class, 'HEADER_MAP'):
                logger.error(f'{job.model_class.__name__} has no HEADER_MAP, cannot create template')
                raise FailureException()
            logger.info(f'Writing template for the {job.model_class.__name__} model to {args.template_file.name}')
            writer = csv.writer(args.template_file)
            writer.writerow(list(job.model_class.HEADER_MAP.values()) + ['FILES', 'ITEM_FILES'])
            return

        if args.import_file is None and not args.resume:
            raise FailureException('An import file is required unless resuming an existing job')

        if args.percentage:
            logger.info(f'Loading {args.percentage}% of the total items')
        if args.validate_only:
            logger.info('Validation-only mode, skipping imports')

        # if an import file was provided, save that as the new CSV metadata file
        if args.import_file is not None:
            job.store_metadata_file(args.import_file)

        try:
            metadata = job.metadata(limit=args.limit, percentage=args.percentage)
        except ModelClassNotFoundError as e:
            raise FailureException(f'Model class {e.model_name} not found') from e
        except JobError as e:
            raise FailureException(str(e)) from e

        if metadata.has_binaries and job.binaries_location is None:
            raise ConfigError('Must specify --binaries-location if the metadata has a FILES column')

        initial_completed_item_count = len(job.completed_log)
        logger.info(f'Found {initial_completed_item_count} completed items')

        updated_uris = []
        created_uris = []
        import_run = job.new_run().start()
        for row in metadata:
            repo_changeset = create_repo_changeset(client, metadata, row)
            item = repo_changeset.item

            # count the number of files referenced in this row
            metadata.files += len(row.filenames)

            try:
                report = validate(item)
            except ValidationError as e:
                raise FailureException(f'Unable to run validation: {e}') from e

            metadata.validation_reports.append({
                'line': row.line_reference,
                'is_valid': report.is_valid(),
                'passed': [outcome for outcome in report.passed()],
                'failed': [outcome for outcome in report.failed()]
            })

            missing_files = [
                name for name in row.filenames if not self.get_source(job.binaries_location, name).exists()
            ]
            if len(missing_files) > 0:
                logger.warning(f'{len(missing_files)} file(s) for "{item}" not found')

            if report.is_valid() and len(missing_files) == 0:
                metadata.valid += 1
                logger.info(f'"{item}" is valid')
            else:
                # drop invalid items
                metadata.invalid += 1
                logger.warning(f'"{item}" is invalid, skipping')
                reasons = [' '.join(str(f) for f in outcome) for outcome in report.failed()]
                if len(missing_files) > 0:
                    reasons.extend(f'Missing file: {f}' for f in missing_files)
                import_run.drop_invalid(
                    item=item,
                    line_reference=row.line_reference,
                    reason=f'Validation failures: {"; ".join(reasons)}'
                )
                continue

            if args.validate_only:
                # validation-only mode
                continue

            try:
                self.update_repo(args, job, client, metadata, row, repo_changeset,
                                 created_uris, updated_uris)
            except FailureException as e:
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

        if args.validate_only:
            # validate phase
            if metadata.invalid == 0:
                result_type = 'validate_success'
            else:
                result_type = 'validate_failed'
        else:
            # import phase
            if len(job.completed_log) == metadata.total:
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
            except RESTAPIException as e:
                raise FailureException(f'Updating item failed: {e}') from e

            job.complete(item, row.line_reference, ImportedItemStatus.MODIFIED)
            metadata.updated += 1
            updated_uris.append(item.uri)

        else:
            job.complete(item, row.line_reference, ImportedItemStatus.UNCHANGED)
            metadata.unchanged += 1
            logger.info(f'No changes found for "{item}" ({row.uri}); skipping')
            metadata.skipped += 1


class RepoChangeset:
    """
    Data object encapsulating the set of changes that need to be made to
    the repository for a single import

    :param item: a repository model object (i.e. from plastron.models) from
                 the repository (or an empty object if validation only)
    :param insert_graph: an RDF Graph object to insert into the repository
    :param delete_graph: an RDF Graph object to delete from the repository
    """
    def __init__(self, item, insert_graph, delete_graph):
        self._item = item
        self._insert_graph = insert_graph
        self._delete_graph = delete_graph

    @property
    def item(self):
        return self._item

    @property
    def insert_graph(self):
        return self._insert_graph

    @property
    def delete_graph(self):
        return self._delete_graph

    @property
    def is_empty(self):
        return len(self.insert_graph) == 0 and len(self.delete_graph) == 0

    def __bool__(self):
        return not self.is_empty

    def build_sparql_update(self, repo):
        return repo.build_sparql_update(self.delete_graph, self.insert_graph)


@rdf.object_property('derived_from', prov.wasDerivedFrom)
class FullTextAnnotation(Annotation):
    pass
