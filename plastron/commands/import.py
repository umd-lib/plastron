import csv
import io
import logging
import os
import plastron.models
import re
from argparse import FileType, Namespace, ArgumentTypeError
from collections import OrderedDict, defaultdict
from datetime import datetime
from os.path import basename, splitext
from plastron import rdf
from plastron.exceptions import DataReadException, NoValidationRulesetException, RESTAPIException, FailureException, \
    ConfigException, BinarySourceNotFoundError
from plastron.files import LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.http import Transaction
from plastron.namespaces import get_manager
from plastron.pcdm import File, Page
from plastron.rdf import RDFDataProperty
from plastron.serializers import CSVSerializer
from plastron.util import uri_or_curie
from rdflib import URIRef, Graph, Literal
from rdflib.util import from_n3
from uuid import uuid4


nsm = get_manager()
logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='import',
        description='Import data to the repository'
    )
    parser.add_argument(
        '-m', '--model',
        help='data model to use',
        required=True,
        action='store'
    )
    parser.add_argument(
        '-l', '--limit',
        help='limit the number of rows to read from the import file',
        type=int,
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
        type=FileType('w'),
        action='store'
    )
    parser.add_argument(
        '--access',
        help='URI or CURIE of the access class to apply to new items',
        type=uri_or_curie,
        action='store'
    )
    parser.add_argument(
        '--member-of',
        help='URI of the object that new items are PCDM members of',
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
        action='store'
    )
    parser.add_argument(
        'import_file', nargs='?',
        help='name of the file to import from',
        type=FileType('r', encoding='utf-8-sig'),
        action='store'
    )
    parser.set_defaults(cmd_name='import')


def build_lookup_index(item, index_string):
    index = defaultdict(dict)
    if index_string is None:
        return index

    # build a lookup index for embedded object properties
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


def get_property_type(model_class: rdf.Resource, attrs):
    if '.' in attrs:
        first, rest = attrs.split('.', 2)
        return get_property_type(model_class.name_to_prop[first].obj_class, rest)
    else:
        return model_class.name_to_prop[attrs]


def build_fields(fieldnames, property_attrs):
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
                attrs = property_attrs[header]
                fields[attrs].append({
                    'header': header,
                    'lang_code': None,
                    'datatype': None
                })
    return fields


def validate(item):
    result = item.validate()

    if result.is_valid():
        logger.info(f'"{item}" is valid')
        for outcome in result.passed():
            logger.debug(f'  ✓ {outcome}')
    else:
        logger.warning(f'{item} is invalid')
        for outcome in result.failed():
            logger.warning(f'  ✗ {outcome}')
        for outcome in result.passed():
            logger.debug(f'  ✓ {outcome}')

    return result


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


def parse_value_string(value_string, column, prop_type):
    # filter out empty strings, so we don't get spurious empty values in the properties
    for value in filter(not_empty, value_string.split('|')):
        if issubclass(prop_type, RDFDataProperty):
            # default to the property's defined datatype
            # if it was not specified in the column header
            yield Literal(value, lang=column['lang_code'], datatype=column.get('datatype', prop_type.datatype))
        else:
            yield URIRef(value)


class Command:
    def __init__(self, config=None):
        self.result = None
        if config is None:
            config = {}
        self.ssh_private_key = config.get('SSH_PRIVATE_KEY')

    def __call__(self, *args, **kwargs):
        for _ in self.execute(*args, **kwargs):
            pass

    def get_source(self, base_location, path):
        """
        Get an appropriate BinarySource based on the type of ``base_location``.
        The following forms of ``base_location`` are recognized:

        * ``zip:<path to zipfile>``
        * ``sftp:<user>@<host>/<path to dir>``
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
        elif base_location.startswith('zip+sftp:'):
            return ZipFileSource(
                zip_file=base_location[4:],
                path=path,
                ssh_options={'key_filename': self.ssh_private_key}
            )
        else:
            # with no URI prefix, assume a local file path
            return LocalFileSource(localpath=os.path.join(base_location, path))

    def add_files(self, item, file_groups, base_location, access=None):
        """
        Add pages and files to the given item. A page is added for each key (basename) in the file_groups
        parameter, and a file is added for each element in the value list for that key.

        :param item: PCDM Object to add the pages to.
        :param file_groups: Dictionary of basename to filename list mappings.
        :param base_location: Location of the files.
        :param access: Optional RDF class representing the access level for this item.
        :return: The number of files added.
        """
        if base_location is None:
            raise ConfigException('Must specify a binaries-location')

        logger.debug(f'Creating {len(file_groups.keys())} page(s)')
        count = 0

        for n, filenames in enumerate(file_groups.values(), 1):
            # create a page object for each rootname
            page = Page(title=f'Page {n}', number=n)
            # add to the item
            item.add_member(page)
            proxy = item.append_proxy(page, title=page.title)
            # add the access class to the page resources
            if access is not None:
                page.rdf_type.append(access)
                proxy.rdf_type.append(access)
            # add the files that are part of this page
            for filename in filenames:
                file = File(self.get_source(base_location, filename), title=filename)
                count += 1
                page.add_file(file)
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
            validate_only=message.args.get('validate-only', False),
            import_file=io.StringIO(message.body),
            template_file=None,
            access=access_uri,
            member_of=message.args.get('member-of'),
            binaries_location=message.args.get('binaries-location')
        )

    def execute(self, repo, args):
        start_time = datetime.now().timestamp()
        try:
            model_class = getattr(plastron.models, args.model)
        except AttributeError:
            logger.error(f'Unknown model: {args.model}')
            raise FailureException()

        if args.template_file is not None:
            if not hasattr(model_class, 'HEADER_MAP'):
                logger.error(f'{model_class.__name__} has no HEADER_MAP, cannot create template')
                raise FailureException()
            logger.info(f'Writing template for the {model_class.__name__} model to {args.template_file.name}')
            writer = csv.writer(args.template_file)
            writer.writerow(list(model_class.HEADER_MAP.values()) + ['FILES'])
            return

        if args.import_file is None:
            logger.info('No import file given')
            return

        if args.validate_only:
            logger.info('Validation-only mode, skipping imports')

        property_attrs = {header: attrs for attrs, header in model_class.HEADER_MAP.items()}

        csv_file = csv.DictReader(args.import_file)

        count = {
            'total': None,
            'rows': 0,
            'errors': 0,
            'valid': 0,
            'invalid': 0,
            'created': 0,
            'updated': 0,
            'unchanged': 0,
            'files': 0
        }

        if args.import_file.seekable():
            # get the row count of the file
            count['total'] = sum(1 for _ in csv_file)
            # rewind the file and re-create the CSV reader
            args.import_file.seek(0)
            csv_file = csv.DictReader(args.import_file)
        else:
            # file is not seekable, so we can't get a row count in advance
            count['total'] = None

        try:
            fields = build_fields(csv_file.fieldnames, property_attrs)
        except DataReadException as e:
            logger.error(str(e))
            raise FailureException(e.message)

        reports = []
        updated_uris = []
        created_uris = []
        for row_number, row in enumerate(csv_file, 1):
            line_reference = f"{getattr(args.import_file, 'name', '<>')}:{row_number + 1}"
            if args.limit is not None and row_number > args.limit:
                logger.info(f'Stopping after {args.limit} rows')
                break
            logger.debug(f'Processing {line_reference}')
            count['rows'] += 1
            if any(v is None for v in row.values()):
                count['errors'] += 1
                error_msg = f'Line {line_reference} has the wrong number of columns'
                reports.append({
                    'line': line_reference,
                    'is_valid': False,
                    'error': error_msg
                })
                logger.warning(f'Skipping: {error_msg}')
                continue

            if 'URI' not in row or row['URI'].strip() == '':
                # no URI in the CSV means we will create a new object
                logger.info(f'No URI found for {line_reference}, creating new object')
                uri = None
            else:
                uri = URIRef(row['URI'])

            if args.validate_only:
                # create an empty object to validate without fetching from the repo
                item = model_class(uri=uri)
            else:
                if uri is not None:
                    # read the object from the repo
                    item = model_class.from_repository(repo, uri, include_server_managed=False)
                else:
                    # create a new object (will create in the repo later)
                    item = model_class()

            index = build_lookup_index(item, row.get('INDEX'))

            # track new embedded objects that are added to the graph
            # so we can ensure that they have at least one statement
            # where they appear as the subject
            new_objects = defaultdict(Graph)

            delete_graph = Graph()
            insert_graph = Graph()

            for attrs, columns in fields.items():
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
                    if first_attr in index:
                        for i, values in new_values.items():
                            # get the embedded object
                            obj = index[first_attr][i]
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
                                index[first_attr][i] = obj
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

            # count the number of files referenced in this row
            if 'FILES' in row and row['FILES'].strip() != '':
                count['files'] += len(row['FILES'].split(';'))

            try:
                report = validate(item)
            except NoValidationRulesetException as e:
                raise FailureException(f'Unable to run validation: {e}') from e

            reports.append({
                'line': line_reference,
                'is_valid': report.is_valid(),
                'passed': [outcome for outcome in report.passed()],
                'failed': [outcome for outcome in report.failed()]
            })

            if report.is_valid():
                count['valid'] += 1
            else:
                # skip invalid items
                count['invalid'] += 1
                logger.warning(f'Skipping "{item}"')
                continue

            if args.validate_only:
                # validation-only mode
                continue

            # construct the SPARQL Update query if there are any deletions or insertions
            if len(delete_graph) > 0 or len(insert_graph) > 0:
                with Transaction(repo) as txn:
                    try:
                        is_new = not item.created
                        if is_new:
                            # if an item is new, don't construct a SPARQL Update query
                            # instead, just create and update normally
                            # create new item in the repo
                            logger.debug('Creating a new item')
                            # add the access class
                            if args.access is not None:
                                item.rdf_type.append(args.access)
                            # add the collection membership
                            if args.member_of is not None:
                                item.member_of = URIRef(args.member_of)

                            if 'FILES' in row and row['FILES'].strip() != '':
                                logger.debug('Adding pages and files to new item')
                                self.add_files(
                                    item,
                                    build_file_groups(row['FILES']),
                                    base_location=args.binaries_location,
                                    access=args.access
                                )

                            item.recursive_create(repo)
                            item.recursive_update(repo)

                            count['created'] += 1

                        else:
                            # do a PATCH update of an existing item
                            logger.info(f'Sending update for {item}')
                            sparql_update = repo.build_sparql_update(delete_graph, insert_graph)
                            logger.debug(sparql_update)
                            item.patch(repo, sparql_update)

                            count['updated'] += 1

                        txn.commit()
                        if is_new:
                            created_uris.append(item.uri)
                        else:
                            updated_uris.append(item.uri)

                    except (RESTAPIException, ConfigException, FailureException, BinarySourceNotFoundError) as e:
                        count['errors'] += 1
                        logger.error(f'{item} import failed: {e}')
                        txn.rollback()
                        logger.warning(f'Rolled back transaction {txn}')
            else:
                count['unchanged'] += 1
                logger.info(f'No changes found for "{item}" ({uri})')

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

        if count['total'] is None:
            # if we weren't able to get the total count before,
            # use the final row count as the total count for the
            # job completion message
            count['total'] = count['rows']

        logger.info(f"Found {count['valid']} valid items")
        logger.info(f"Found {count['invalid']} invalid items")
        logger.info(f"Found {count['errors']} errors")
        if not args.validate_only:
            logger.info(f"{count['unchanged']} of {count['total']} items remained unchanged")
            logger.info(f"Created {count['created']} of {count['total']} items")
            logger.info(f"Updated {count['updated']} of {count['total']} items")
        self.result = {
            'count': count,
            'validation': reports,
            'uris': {
                'created': created_uris,
                'updated': updated_uris
            }
        }
