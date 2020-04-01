import csv
import logging
import plastron.models
import re

from collections import defaultdict
from datetime import datetime
from operator import attrgetter
from plastron.exceptions import FailureException, NoValidationRulesetException, DataReadException
from plastron.rdf import RDFDataProperty
from plastron.serializers import CSVSerializer
from rdflib import URIRef, Graph, Literal

logger = logging.getLogger(__name__)


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
        '--validate-only',
        help='only validate, do not do the actual import',
        action='store_true'
    )
    parser.add_argument(
        'filename', nargs=1,
        help='name of the file to import from'
    )
    parser.set_defaults(cmd_name='import')


def build_lookup_index(item, index_string):
    if index_string is None:
        return {}

    # build a lookup index for embedded object properties
    index = defaultdict(dict)
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
            # no language tag
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
        return True
    else:
        logger.warning(f'{item} is invalid')
        for outcome in result.failed():
            logger.warning(f'  ✗ {outcome}')
        for outcome in result.passed():
            logger.debug(f'  ✓ {outcome}')
        return False


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

        csv_filename = args.filename[0]

        if args.validate_only:
            logger.info('Validation-only mode, skipping imports')

        property_attrs = {header: attrs for attrs, header in model_class.HEADER_MAP.items()}

        with open(csv_filename) as file:
            csv_file = csv.DictReader(file)
            fields = build_fields(csv_file.fieldnames, property_attrs)

            row_count = 0
            updated_count = 0
            unchanged_count = 0
            invalid_count = 0
            error_count = 0
            for row_number, row in enumerate(csv_file, 1):
                line_reference = f'{csv_filename}:{row_number + 1}'
                if args.limit is not None and row_number > args.limit:
                    logger.info(f'Stopping after {args.limit} rows')
                    break
                logger.debug(f'Processing {line_reference}')
                if any(v is None for v in row.values()):
                    error_count += 1
                    logger.warning(f'Line {line_reference} has the wrong number of columns; skipping')
                    continue

                uri = URIRef(row['URI'])
                row_count += 1

                # read the object from the repo
                item = model_class.from_graph(repo.get_graph(uri, False), uri)

                index = build_lookup_index(item, row.get('INDEX'))

                delete_graph = Graph()
                insert_graph = Graph()
                for attrs, columns in fields.items():
                    prop = attrgetter(attrs)(item)
                    new_values = []
                    for column in columns:
                        header = column['header']
                        language_code = column['lang_code']
                        datatype = column['datatype']
                        values = [v for v in row[header].split('|') if len(v.strip()) > 0]

                        if isinstance(prop, RDFDataProperty):
                            new_values.extend(Literal(v, lang=language_code, datatype=datatype) for v in values)
                        else:
                            new_values = [URIRef(v) for v in values]

                    # construct a SPARQL update by diffing for deletions and insertions
                    if '.' not in attrs:
                        # simple, non-embedded values
                        # update the property and get the sets of values deleted and inserted
                        deleted_values, inserted_values = prop.update(new_values)

                        for deleted_value in deleted_values:
                            delete_graph.add((uri, prop.uri, prop.get_term(deleted_value)))
                        for inserted_value in inserted_values:
                            insert_graph.add((uri, prop.uri, prop.get_term(inserted_value)))

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
                                # TODO: deal with additional new values that don't correspond to old
                                old_value = getattr(obj, next_attr).values[0]
                                if new_value != old_value:
                                    setattr(obj, next_attr, new_value)
                                    delete_graph.add((obj.uri, prop.uri, prop.get_term(old_value)))
                                    insert_graph.add((obj.uri, prop.uri, prop.get_term(new_value)))

                # do a pass to remove statements that are both deleted and then re-inserted
                for statement in delete_graph:
                    if statement in insert_graph:
                        delete_graph.remove(statement)
                        insert_graph.remove(statement)

                if not validate(item):
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
                        'errors': error_count
                    }
                }

        logger.info(f'{unchanged_count} of {row_count} items remained unchanged')
        logger.info(f'Updated {updated_count} of {row_count} items')
        logger.info(f'Found {invalid_count} invalid items')
        logger.info(f'Found {error_count} errors')
        self.result = {
            'count': {
                'total': row_count,
                'updated': updated_count,
                'unchanged': unchanged_count,
                'invalid': invalid_count,
                'errors': error_count
            }
        }
