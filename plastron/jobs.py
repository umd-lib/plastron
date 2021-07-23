import csv
import logging
import os
import re
import urllib.parse
from collections import defaultdict
from shutil import copyfileobj
from typing import List, Mapping

import yaml
from rdflib import URIRef
from rdflib.util import from_n3

import plastron.models
from plastron.exceptions import DataReadException, FailureException
from plastron.namespaces import get_manager
from plastron.rdf import RDFDataProperty, Resource
from plastron.serializers import CSVSerializer
from plastron.util import ItemLog, datetimestamp


nsm = get_manager()
logger = logging.getLogger(__name__)


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


class ImportJob:
    def __init__(self, id, jobs_dir):
        self.id = id
        # URL-escaped ID that can be used as a path segment on a filesystem or URL
        self.safe_id = urllib.parse.quote(id, safe='')
        # use a timestamp to differentiate different runs of the same import job
        self.run_timestamp = datetimestamp()
        self.dir = os.path.join(jobs_dir, self.safe_id)
        self.config_filename = os.path.join(self.dir, 'config.yml')
        self.metadata_filename = os.path.join(self.dir, 'source.csv')
        self.config = {}
        self._model_class = None

        # record of items that are successfully loaded
        completed_fieldnames = ['id', 'timestamp', 'title', 'uri']
        self.completed_log = ItemLog(os.path.join(self.dir, 'completed.log.csv'), completed_fieldnames, 'id')

        # record of items that are unable to be loaded during this import run
        dropped_basename = f'dropped-{self.run_timestamp}'
        dropped_log_filename = os.path.join(self.dir, f'{dropped_basename}.log.csv')
        dropped_fieldnames = ['id', 'timestamp', 'title', 'uri', 'reason']
        self.dropped_log = ItemLog(dropped_log_filename, dropped_fieldnames, 'id')

    def __getattr__(self, item):
        try:
            return self.config[item]
        except KeyError:
            raise AttributeError(f'No attribute or config key named {item} found')

    @property
    def dir_exists(self):
        return os.path.isdir(self.dir)

    def load_config(self):
        with open(self.config_filename) as config_file:
            self.config = yaml.safe_load(config_file)
        return self.config

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

    def drop(self, item, line_reference, reason=''):
        logger.warning(f'Dropping {line_reference} from import job "{self.id}" run {self.run_timestamp}: {reason}')
        self.dropped_log.append({
            'id': getattr(item, 'identifier', line_reference),
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(item, 'title', ''),
            'uri': getattr(item, 'uri', ''),
            'reason': reason
        })

    def complete(self, item, line_reference):
        # write to the completed item log
        self.completed_log.append({
            'id': getattr(item, 'identifier', line_reference),
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(item, 'title', ''),
            'uri': getattr(item, 'uri', '')
        })

    def metadata(self, **kwargs):
        return MetadataRows(self, **kwargs)


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
            raise FailureException(f'Cannot read source file "{job.metadata_filename}: {e}') from e

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
                self.job.drop(item=None, line_reference=line_reference, reason='Wrong number of columns')
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
    def filenames(self):
        return self.data['FILES'].strip().split(';') if self.has_files else []

    @property
    def index_string(self):
        return self.data.get('INDEX')
