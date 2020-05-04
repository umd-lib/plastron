import csv
import io
import logging
import os
import plastron.models
import re
from argparse import FileType, Namespace, ArgumentTypeError
from collections import defaultdict
from datetime import datetime
from operator import attrgetter
from plastron import rdf
from plastron.exceptions import DataReadException, NoValidationRulesetException, RESTAPIException, FailureException, \
    ConfigException, BinarySourceNotFoundError
from plastron.files import LocalFile, RemoteFile, ZipFile
from plastron.http import Transaction
from plastron.pcdm import File, Page
from plastron.rdf import RDFDataProperty
from plastron.serializers import CSVSerializer
from plastron.util import uri_or_curie
from rdflib import URIRef, Graph, Literal
from uuid import uuid4


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
        help='where to find binaries; either a path to a directory or a "zip:<path to zipfile>" URI',
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
        index[attr][i] = getattr(item, attr)[URIRef(item.uri + uriref)]
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
            # header format is "Header Label [Language Name]"
            m = re.search(r'^([^[]+)\s+\[(.+)\]$', header)
            if m[1] not in property_attrs:
                raise DataReadException(f'Unrecognized header "{header}" in import file.')
            attrs = property_attrs[m[1]]
            if m[2] not in CSVSerializer.LANGUAGE_CODES:
                raise DataReadException(f'Unrecognized language "{m[2]}" in "{header}" in import file.')
            lang_code = CSVSerializer.LANGUAGE_CODES[m[2]]
            fields[attrs].append({
                'header': header,
                'lang_code': lang_code,
                'datatype': None
            })
        elif '{' in header:
            # this field has a datatype
            # header format is "Header Label {Datatype Name}
            m = re.search(r'^([^{]+)\s+{(.+)}$', header)
            if m[1] not in property_attrs:
                raise DataReadException(f'Unrecognized header "{header}" in import file.')
            attrs = property_attrs[m[1]]
            if m[2] not in CSVSerializer.DATATYPE_URIS:
                raise DataReadException(f'Unrecognized datatype "{m[2]}" in "{header}" in import file.')
            datatype_uri = CSVSerializer.DATATYPE_URIS[m[2]]
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
    try:
        result = item.validate()
    except NoValidationRulesetException as e:
        logger.error(f'Unable to run validation: {e.message}')
        raise FailureException()

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


def get_source(base_location, path):
    if base_location.startswith('zip:'):
        return ZipFile(base_location[4:], path)
    elif base_location.startswith('sftp:'):
        return RemoteFile(os.path.join(base_location, path))
    else:
        # with no URI prefix, assume a local file path
        return LocalFile(localpath=os.path.join(base_location, path))


def add_files(item, filenames, base_location, access=None):
    if base_location is None:
        raise ConfigException('Must specify a binaries-location')

    for n, filename in enumerate(filenames, 1):
        file = File(get_source(base_location, filename), title=filename)
        # create page objects and add a file for each
        page = Page(title=f'Page {n}', number=n)
        page.add_file(file)
        # add to the item
        item.add_member(page)
        proxy = item.append_proxy(page, title=page.title)
        # add the access class to the page resources
        if access is not None:
            file.rdf_type.append(access)
            page.rdf_type.append(access)
            proxy.rdf_type.append(access)


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


class Command:
    def __init__(self):
        self.result = None

    def __call__(self, *args, **kwargs):
        for _ in self.execute(*args, **kwargs):
            pass

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

        if args.import_file.seekable():
            # get the row count of the file
            total_count = sum(1 for _ in csv_file)
            # rewind the file and re-create the CSV reader
            args.import_file.seek(0)
            csv_file = csv.DictReader(args.import_file)
        else:
            # file is not seekable, so we can't get a row count in advance
            total_count = None

        try:
            fields = build_fields(csv_file.fieldnames, property_attrs)
        except DataReadException as e:
            logger.error(str(e))
            raise FailureException(e.message)

        row_count = 0
        updated_count = 0
        unchanged_count = 0
        invalid_count = 0
        valid_count = 0
        error_count = 0
        reports = []
        updated_uris = []
        created_uris = []
        for row_number, row in enumerate(csv_file, 1):
            line_reference = f"{getattr(args.import_file, 'name', '<>')}:{row_number + 1}"
            if args.limit is not None and row_number > args.limit:
                logger.info(f'Stopping after {args.limit} rows')
                break
            logger.debug(f'Processing {line_reference}')
            row_count += 1
            if any(v is None for v in row.values()):
                error_count += 1
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

            delete_graph = Graph()
            insert_graph = Graph()
            for attrs, columns in fields.items():
                prop = attrgetter(attrs)(item)
                prop_type = get_property_type(item.__class__, attrs)
                new_values = []
                for column in columns:
                    header = column['header']
                    language_code = column['lang_code']
                    datatype = column['datatype']
                    values = [v for v in row[header].split('|') if len(v.strip()) > 0]

                    if issubclass(prop_type, RDFDataProperty):
                        if datatype is None:
                            # default to the property's defined datatype
                            # if it was not specified in the column header
                            datatype = prop_type.datatype
                        new_values.extend(Literal(v, lang=language_code, datatype=datatype) for v in values)
                    else:
                        new_values = [URIRef(v) for v in values]

                # construct a SPARQL update by diffing for deletions and insertions
                if '.' not in attrs:
                    # simple, non-embedded values
                    # update the property and get the sets of values deleted and inserted
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
                    if first_attr in index:
                        for i, new_value in enumerate(new_values):
                            # get the embedded object
                            obj = index[first_attr][i]
                            try:
                                old_value = getattr(obj, next_attr).values[0]
                            except IndexError:
                                old_value = None
                            if new_value != old_value:
                                setattr(obj, next_attr, new_value)
                                if old_value is not None:
                                    delete_graph.add((obj.uri, prop.uri, prop.get_term(old_value)))
                                insert_graph.add((obj.uri, prop.uri, prop.get_term(new_value)))
                    else:
                        # add new hash objects that are not in the index
                        prop_type = item.name_to_prop[first_attr]
                        for i, new_value in enumerate(new_values):
                            # we can assume that for any properties with dotted notation,
                            # all attributes except for the last one are object properties
                            if prop_type.obj_class is not None:
                                # create a new object
                                # TODO: remove hardcoded UUID fragment minting
                                obj = prop_type.obj_class(uri=f'{item.uri}#{uuid4()}')
                                # add the new object to the index
                                index[first_attr][0] = obj
                                setattr(obj, next_attr, new_value)
                                next_attr_prop = obj.name_to_prop[next_attr]
                                # add that object to the main item
                                getattr(item, first_attr).append(obj)
                                insert_graph.add((item.uri, prop_type.uri, obj.uri))
                                insert_graph.add((obj.uri, next_attr_prop.uri, next_attr_prop.get_term(new_value)))

            # do a pass to remove statements that are both deleted and then re-inserted
            for statement in delete_graph:
                if statement in insert_graph:
                    delete_graph.remove(statement)
                    insert_graph.remove(statement)

            report = validate(item)
            reports.append({
                'line': line_reference,
                'is_valid': report.is_valid(),
                'passed': [outcome for outcome in report.passed()],
                'failed': [outcome for outcome in report.failed()]
            })

            if report.is_valid():
                valid_count += 1
            else:
                # skip invalid items
                invalid_count += 1
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
                                add_files(item, row['FILES'].split(';'), args.binaries_location, args.access)

                            item.recursive_create(repo)
                            item.recursive_update(repo)

                        else:
                            # do a PATCH update of an existing item
                            logger.info(f'Sending update for {item}')
                            sparql_update = repo.build_sparql_update(delete_graph, insert_graph)
                            logger.debug(sparql_update)
                            item.patch(repo, sparql_update)

                        txn.commit()
                        updated_count += 1
                        if is_new:
                            created_uris.append(item.uri)
                        else:
                            updated_uris.append(item.uri)

                    except (RESTAPIException, ConfigException, BinarySourceNotFoundError) as e:
                        error_count += 1
                        logger.error(f'{item} import failed: {e}')
                        txn.rollback()
                        logger.warning(f'Rolled back transaction {txn}')
            else:
                unchanged_count += 1
                logger.info(f'No changes found for "{item}" ({uri})')

            # update the status
            now = datetime.now().timestamp()
            yield {
                'time': {
                    'started': start_time,
                    'now': now,
                    'elapsed': now - start_time
                },
                'count': {
                    'total': total_count,
                    'updated': updated_count,
                    'unchanged': unchanged_count,
                    'valid': valid_count,
                    'invalid': invalid_count,
                    'errors': error_count
                }
            }

        if total_count is None:
            # if we weren't able to get the total count before,
            # use the final row count as the total count for the
            # job completion message
            total_count = row_count

        logger.info(f'Found {valid_count} valid items')
        logger.info(f'Found {invalid_count} invalid items')
        logger.info(f'Found {error_count} errors')
        if not args.validate_only:
            logger.info(f'{unchanged_count} of {total_count} items remained unchanged')
            logger.info(f'Updated {updated_count} of {total_count} items')
        self.result = {
            'count': {
                'total': total_count,
                'updated': updated_count,
                'unchanged': unchanged_count,
                'valid': valid_count,
                'invalid': invalid_count,
                'errors': error_count
            },
            'validation': reports,
            'uris': {
                'created': created_uris,
                'updated': updated_uris
            }
        }
