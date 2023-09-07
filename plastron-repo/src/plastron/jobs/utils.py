import csv
import logging
import re
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from os.path import splitext, basename
from typing import Optional, Dict, List, Union, Mapping, NamedTuple, Type

from bs4 import BeautifulSoup
from rdflib import Literal, URIRef, Graph
from rdflib.util import from_n3

from plastron.client import Client
from plastron.namespaces import sc, get_manager
from plastron.rdf import rdf
from plastron.rdf.oa import TextualBody, FullTextAnnotation
from plastron.rdfmapping.descriptors import DataProperty, Property
from plastron.rdfmapping.properties import RDFObjectProperty
from plastron.rdfmapping.resources import RDFResourceBase
from plastron.repo import DataReadError, Repository, RepositoryResource
from plastron.serializers import CSVSerializer

logger = logging.getLogger(__name__)
nsm = get_manager()


def get_property_type(model_class: rdf.Resource, attrs):
    if '.' in attrs:
        first, rest = attrs.split('.', 2)
        return get_property_type(model_class.name_to_prop[first].obj_class, rest)
    else:
        return model_class.name_to_prop[attrs]


def build_lookup_index(item: RDFResourceBase, index_string: str):
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


class JobError(Exception):
    def __init__(self, job, *args):
        super().__init__(*args)
        self.job = job

    def __str__(self):
        return f'Job {self.job} error: {super().__str__()}'


class JobConfigError(JobError):
    pass


class MetadataError(JobError):
    pass


class LineReference(NamedTuple):
    filename: str
    line_number: int

    def __str__(self):
        return f'{self.filename}:{self.line_number}'


@dataclass
class ColumnSpec:
    attrs: str
    header: str
    prop: Property
    lang_code: Optional[str] = None
    datatype: Optional[URIRef] = None


def build_fields(fieldnames, model_class) -> Dict[str, List[ColumnSpec]]:
    property_attrs = {header: attrs for attrs, header in model_class.HEADER_MAP.items()}
    fields = defaultdict(list)
    # group typed and language-tagged columns by their property attribute
    for header in fieldnames:
        # make sure we skip the system columns
        if header in CSVSerializer.SYSTEM_HEADERS:
            continue

        if '[' in header:
            # this field has a language tag
            # header format is "Header Label [Language Label]"
            header_label, language_label = re.search(r'^([^[]+)\s+\[(.+)]$', header).groups()
            try:
                attrs = property_attrs[header_label]
            except KeyError as e:
                raise DataReadError(f'Unknown header "{header}" in import file.') from e
            # if the language label isn't a name in the LANGUAGE_CODES table,
            # assume that it is itself a language code
            lang_code = CSVSerializer.LANGUAGE_CODES.get(language_label, language_label)
            fields[attrs].append(ColumnSpec(
                attrs=attrs,
                header=header,
                prop=get_final_prop(model_class, attrs),
                lang_code=lang_code,
                datatype=None,
            ))
        elif '{' in header:
            # this field has a datatype
            # header format is "Header Label {Datatype Label}
            header_label, datatype_label = re.search(r'^([^{]+)\s+{(.+)}$', header).groups()
            try:
                attrs = property_attrs[header_label]
            except KeyError as e:
                raise DataReadError(f'Unknown header "{header}" in import file.') from e
            # the datatype label should either be a key in the lookup table,
            # or an n3-abbreviated URI of a datatype
            try:
                datatype_uri = CSVSerializer.DATATYPE_URIS.get(datatype_label, from_n3(datatype_label, nsm=nsm))
                if not isinstance(datatype_uri, URIRef):
                    raise DataReadError(f'Unknown datatype "{datatype_label}" in "{header}" in import file.')
            except KeyError as e:
                raise DataReadError(f'Unknown datatype "{datatype_label}" in "{header}" in import file.') from e

            fields[attrs].append(ColumnSpec(
                attrs=attrs,
                header=header,
                prop=get_final_prop(model_class, attrs),
                lang_code=None,
                datatype=datatype_uri,
            ))
        else:
            # no language tag or datatype
            if header not in property_attrs:
                raise DataReadError(f'Unrecognized header "{header}" in import file.')
            # check for a default datatype defined in the model
            attrs = property_attrs[header]
            prop = get_final_prop(model_class, attrs.split('.'))
            if prop is not None and isinstance(prop, DataProperty):
                datatype_uri = prop.datatype
            else:
                datatype_uri = None
            fields[attrs].append(ColumnSpec(
                attrs=attrs,
                header=header,
                prop=prop,
                lang_code=None,
                datatype=datatype_uri,
            ))
    return fields


def get_final_prop(model_class, attrs):
    next_attr_name = attrs.pop(0)
    next_attr = getattr(model_class, next_attr_name)
    if not attrs:
        return next_attr
    return get_final_prop(next_attr.object_class, attrs)


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


def parse_value_string(value_string, column: ColumnSpec) -> List[Union[Literal, URIRef]]:
    values = []
    # filter out empty strings, so we don't get spurious empty values in the properties
    for value in filter(not_empty, split_escaped(value_string, separator='|')):
        if isinstance(column.prop, DataProperty):
            # default to the property's defined datatype
            # if it was not specified in the column header
            values.append(Literal(value, lang=column.lang_code, datatype=(column.datatype or column.prop.datatype)))
        else:
            values.append(URIRef(value))
    return values


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


class Row:
    def __init__(
            self,
            spreadsheet: 'MetadataRows',
            line_reference: LineReference,
            row_number: int,
            data: Mapping,
            identifier_column: str,
    ):
        self.spreadsheet = spreadsheet
        self.line_reference = line_reference
        self.number = row_number
        self.data = data
        self.identifier_column = identifier_column

    def __getitem__(self, item):
        return self.data[item]

    def get(self, key, default=None):
        return self.data.get(key, default)

    def parse_value(self, column: ColumnSpec) -> List[Union[Literal, URIRef]]:
        return parse_value_string(self[column.header], column)

    def get_object(self, repo: Repository, read_from_repo: bool = False):
        if self.uri is not None:
            # resource with the URI from the spreadsheet
            resource = repo[self.uri]
            if read_from_repo:
                # unless we are only validating,
                # read the object from the repo
                resource.read()
        else:
            # no URI in the CSV means we will create a new object
            logger.info(f'No URI found for {self.line_reference}; will create new resource')
            # create a new object (will create in the repo later)
            resource = RepositoryResource(repo=repo)

        return resource.describe(self.spreadsheet.model_class)

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
    def item_filenames(self):
        return self.data['ITEM_FILES'].strip().split(';') if self.has_item_files else []

    @property
    def index_string(self):
        return self.data.get('INDEX')


class MetadataRows:
    """
    Iterable sequence of rows from the metadata CSV file of an import job.
    """

    def __init__(self, job, limit: int = None, percentage: int = None):
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
        except DataReadError as e:
            raise RuntimeError(str(e)) from e

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
                raise RuntimeError('Cannot execute a percentage load using a non-seekable file')
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
    def model_class(self) -> Type[RDFResourceBase]:
        return self.job.model_class

    @property
    def has_binaries(self) -> bool:
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

            line_reference = LineReference(filename=str(self.job.metadata_filename), line_number=row_number + 1)
            logger.debug(f'Processing {line_reference}')
            self.rows += 1

            if any(v is None for v in line.values()):
                self.errors += 1
                self.validation_reports.append({
                    'line': line_reference,
                    'is_valid': False,
                    'error': f'Line {line_reference} has the wrong number of columns'
                })
                # TODO: this should be part of ImportRun?
                self.job.drop_invalid(item=None, line_reference=line_reference, reason='Wrong number of columns')
                continue

            row = Row(self, line_reference, row_number, line, self.identifier_column)

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


def create_repo_changeset(repo: Repository, row: Row, validate_only=False):
    """
    Returns a RepoChangeset of the changes to make to the repository

    :param repo: the repository configuration
    :param row: A single plastron.jobs.Row object representing the row
                 to import
    :param validate_only: If true, will not fetch existing object from the
                repository.
    :return: A RepoChangeSet encapsulating the changes to make to the
            repository.
    """
    fields = row.spreadsheet.fields
    item = row.get_object(repo, read_from_repo=(not validate_only))

    # track new embedded objects that are added to the graph
    # so we can ensure that they have at least one statement
    # where they appear as the subject
    # new_objects = defaultdict(Graph)

    # build the lookup index to map hash URI objects
    # to their correct positional locations
    row_index = build_lookup_index(item, row.index_string)

    for attrs, columns in fields.items():  # type: str, List[ColumnSpec]
        if '.' not in attrs:
            # simple, non-embedded values
            # attrs is the entire property name
            new_values = []
            # there may be 1 or more physical columns in the metadata spreadsheet
            # that correspond to a single logical property of the item (e.g., multiple
            # languages or datatypes)
            for column in columns:  # type: ColumnSpec
                new_values.extend(row.parse_value(column))

            # construct a SPARQL update by diffing for deletions and insertions
            # update the property and get the sets of values deleted and inserted
            prop = getattr(item, attrs)
            prop.update(new_values)

        else:
            # complex, embedded values

            # if the first portion of the dotted attr notation is a key in the index,
            # then this column has a different subject than the main uri
            # correlate positions and urirefs
            # XXX: for now, assuming only 2 levels of chaining
            first_attr, next_attrs = attrs.split('.', 1)
            new_values = defaultdict(list)
            for column in columns:  # type: ColumnSpec
                for i, value_string in enumerate(row[column.header].split(';')):
                    new_values[i].extend(parse_value_string(value_string, column))

            if first_attr in row_index:
                # existing embedded object
                for i, values in new_values.items():
                    # get the embedded object
                    obj = row_index[first_attr][i]
                    prop = getattr(obj, next_attrs)
                    prop.update(values)
            else:
                # create new embedded objects (a.k.a hash resources) that are not in the index
                first_prop: RDFObjectProperty = getattr(item, first_attr)
                for i, values in new_values.items():
                    # we can assume that for any properties with dotted notation,
                    # all attributes except for the last one are object properties
                    if first_prop.object_class is not None:
                        # create a new object
                        obj = item.get_fragment_resource(first_prop.object_class)
                        # add the new object to the index
                        row_index[first_attr][i] = obj
                        getattr(obj, next_attrs).extend(values)
                        first_prop.add(obj)

    return item


class RepoChangeset:
    """
    Data object encapsulating the set of changes that need to be made to
    the repository for a single import

    :param item: a repository model object (i.e. from plastron.models) from
                 the repository (or an empty object if validation only)
    """
    def __init__(self, item):
        self._item = item
        self._insert_graph = Graph()
        for triple in self._item.inserts:
            self._insert_graph.add(triple)
        self._delete_graph = Graph()
        for triple in self._item.deletes:
            self._delete_graph.add(triple)

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

    def build_sparql_update(self, client: Client):
        return client.build_sparql_update(self.delete_graph, self.insert_graph)
