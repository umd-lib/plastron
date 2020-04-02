import csv
import io
import json
import logging
import plastron.models
import re
from argparse import FileType, Namespace
from collections import defaultdict
from datetime import datetime
from operator import attrgetter
from plastron import rdf
from plastron.exceptions import FailureException, NoValidationRulesetException, RESTAPIException
from plastron.rdf import RDFDataProperty
from plastron.serializers import CSVSerializer
from plastron.stomp import Message
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
        'import_file', nargs='?',
        help='name of the file to import from',
        type=FileType('r'),
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


def build_sparql_update(delete_graph, insert_graph):
    deletes = delete_graph.serialize(format='nt').decode('utf-8').strip()
    inserts = insert_graph.serialize(format='nt').decode('utf-8').strip()
    sparql_update = f"DELETE {{ {deletes} }} INSERT {{ {inserts} }} WHERE {{}}"
    return sparql_update


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
            attrs = property_attrs[m[1]]
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
            attrs = property_attrs[m[1]]
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
            writer.writerow(model_class.HEADER_MAP.values())
            return

        if args.import_file is None:
            logger.info('No import file given')
            return

        if args.validate_only:
            logger.info('Validation-only mode, skipping imports')

        property_attrs = {header: attrs for attrs, header in model_class.HEADER_MAP.items()}

        csv_file = csv.DictReader(args.import_file)
        fields = build_fields(csv_file.fieldnames, property_attrs)

        row_count = 0
        updated_count = 0
        unchanged_count = 0
        invalid_count = 0
        valid_count = 0
        error_count = 0
        reports = []
        for row_number, row in enumerate(csv_file, 1):
            line_reference = f"{getattr(args.import_file, 'name', '<>')}:{row_number + 1}"
            if args.limit is not None and row_number > args.limit:
                logger.info(f'Stopping after {args.limit} rows')
                break
            logger.debug(f'Processing {line_reference}')
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

            row_count += 1
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
                    item = model_class.from_graph(repo.get_graph(uri, False), uri)
                else:
                    # create a new object in the repo
                    item = model_class()
                    item.create_object(repo)

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
                logger.info(f'Sending update for {item}')
                sparql_update = build_sparql_update(delete_graph, insert_graph)
                logger.debug(sparql_update)
                item.patch(repo, sparql_update)
                updated_count += 1
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
                    'total': row_count,
                    'updated': updated_count,
                    'unchanged': unchanged_count,
                    'valid': valid_count,
                    'invalid': invalid_count,
                    'errors': error_count
                }
            }

        logger.info(f'Found {valid_count} valid items')
        logger.info(f'Found {invalid_count} invalid items')
        logger.info(f'Found {error_count} errors')
        if not args.validate_only:
            logger.info(f'{unchanged_count} of {row_count} items remained unchanged')
            logger.info(f'Updated {updated_count} of {row_count} items')
        self.result = {
            'count': {
                'total': row_count,
                'updated': updated_count,
                'unchanged': unchanged_count,
                'valid': valid_count,
                'invalid': invalid_count,
                'errors': error_count
            },
            'validation': reports
        }


def process_message(listener, message):

    # define the processor for this message
    def process():
        if message.job_id is None:
            logger.error('Expecting a PlastronJobId header')
        else:
            logger.info(f'Received message to initiate import job {message.job_id}')

            try:
                command = Command()
                args = Namespace(
                    model=message.args.get('model'),
                    limit=message.args.get('limit', None),
                    validate_only=message.args.get('validate-only', False),
                    import_file=io.StringIO(message.body),
                    template_file=None
                )

                for status in command.execute(listener.repository, args):
                    listener.broker.connection.send(
                        '/topic/plastron.jobs.status',
                        headers={
                            'PlastronJobId': message.job_id
                        },
                        body=json.dumps(status)
                    )

                logger.info(f'Import job {message.job_id} complete')

                return Message(
                    headers={
                        'PlastronJobId': message.job_id,
                        'PlastronJobStatus': 'Done',
                        'persistent': 'true'
                    },
                    body=json.dumps(command.result)
                )

            except (FailureException, RESTAPIException) as e:
                logger.error(f"Export job {message.job_id} failed: {e}")
                return Message(
                    headers={
                        'PlastronJobId': message.job_id,
                        'PlastronJobStatus': 'Failed',
                        'PlastronJobError': str(e),
                        'persistent': 'true'
                    }
                )

    # process message
    listener.executor.submit(process).add_done_callback(listener.get_response_handler(message.id))
