import csv
import logging
import os
import re
import urllib.parse
from collections import defaultdict
from enum import Enum
from pathlib import Path
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


def is_run_dir(path: Path) -> bool:
    return path.is_dir() and re.match(r'^\d{14}$', path.name)


class JobError(Exception):
    def __init__(self, job, *args):
        super().__init__(*args)
        self.job = job

    def __str__(self):
        return f'Job {self.job} error: {super().__str__()}'


class ConfigMissingError(JobError):
    pass


class MetadataError(JobError):
    pass


class ImportedItemStatus(Enum):
    CREATED = 'created'
    MODIFIED = 'modified'
    UNCHANGED = 'unchanged'


class ImportJob:
    def __init__(self, job_id, jobs_dir):
        self.id = job_id
        # URL-escaped ID that can be used as a path segment on a filesystem or URL
        self.safe_id = urllib.parse.quote(job_id, safe='')
        # use a timestamp to differentiate different runs of the same import job
        self.run_timestamp = datetimestamp()
        self.dir = Path(jobs_dir) / self.safe_id
        self.config_filename = self.dir / 'config.yml'
        self.metadata_filename = self.dir / 'source.csv'
        self.config = {}
        self._model_class = None

        # record of items that are successfully loaded
        completed_fieldnames = ['id', 'timestamp', 'title', 'uri', 'status']
        self.completed_log = ItemLog(self.dir / 'completed.log.csv', completed_fieldnames, 'id')

    def __str__(self):
        return self.id

    def __getattr__(self, item):
        try:
            return self.config[item]
        except KeyError:
            raise AttributeError(f'No attribute or config key named {item} found')

    @property
    def dir_exists(self):
        return self.dir.is_dir()

    def load_config(self):
        try:
            with open(self.config_filename) as config_file:
                config = yaml.safe_load(config_file)
            if config is None:
                raise ConfigMissingError(self, f'Config file {self.config_filename} is empty')
            for key, value in config.items():
                # catch any improperly serialized "None" values, and convert to None
                if value == 'None':
                    config[key] = None
            self.config = config
            return self.config
        except FileNotFoundError as e:
            raise ConfigMissingError(self, f'Config file {self.config_filename} is missing') from e

    def save_config(self, config):
        # store the relevant config
        self.config = config

        # if we are not resuming, make sure the directory exists
        os.makedirs(self.dir, exist_ok=True)
        with open(self.config_filename, mode='w') as config_file:
            yaml.dump(
                stream=config_file,
                data={'job_id': self.id, **{k: str(v) if v is not None else v for k, v in self.config.items()}}
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

    def complete(self, item: Resource, line_reference: str, status: ImportedItemStatus):
        # write to the completed item log
        self.completed_log.append({
            'id': getattr(item, 'identifier', line_reference),
            'timestamp': datetimestamp(digits_only=False),
            'title': getattr(item, 'title', ''),
            'uri': getattr(item, 'uri', ''),
            'status': status.value
        })

    def metadata(self, **kwargs):
        return MetadataRows(self, **kwargs)

    def new_run(self):
        return ImportRun(self)

    def get_run(self, timestamp=None):
        if timestamp is None:
            # get the latest run
            return self.latest_run()
        else:
            return ImportRun(self).load(timestamp)

    @property
    def runs(self):
        return sorted((d.name for d in filter(is_run_dir, self.dir.iterdir())), reverse=True)

    def latest_run(self):
        try:
            return ImportRun(self).load(self.runs[0])
        except IndexError:
            return None


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
            raise FailureException(f'Import run {self.timestamp} not found')
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
            raise FailureException('Run completed, cannot start again')
        self.timestamp = datetimestamp()
        self.dir = self.job.dir / self.timestamp
        os.makedirs(self.dir)
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
    def has_item_files(self):
        return 'ITEM_FILES' in self.data and self.data['ITEM_FILES'].strip() != ''

    @property
    def filenames(self):
        return self.data['FILES'].strip().split(';') if self.has_files else []

    @property
    def index_string(self):
        return self.data.get('INDEX')
