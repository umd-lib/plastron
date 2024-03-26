import csv
import os
import re
from collections import defaultdict
from contextlib import contextmanager
from itertools import zip_longest
from pathlib import Path
from typing import List, Union, Dict, Type, NamedTuple, Mapping, Iterable
from urllib.parse import urlparse

from rdflib import URIRef, Literal
from urlobject import URLObject

from plastron.models.pcdm import PCDMFile
from plastron.namespaces import fedora
from plastron.rdfmapping.descriptors import ObjectProperty
from plastron.rdfmapping.embed import EmbeddedObject
from plastron.rdfmapping.properties import RDFObjectProperty, RDFDataProperty
from plastron.rdfmapping.resources import RDFResourceBase
from plastron.repo import BinaryResource


def not_empty(value):
    """Returns true if `value` is not `None` and is not the empty string."""
    return value is not None and value != ''


def split_escaped(string: str, separator: str = '|') -> List[str]:
    """Split a string on the separator, taking into account escaped instances
    of the separator.

    ```pycon
    >>> split_escaped('foo|bar')
    ['foo', 'bar']

    >>> split_escaped('foo\|bar|alpha')
    ['foo|bar', 'alpha']
    ```
    """
    if string is None or string == '':
        return []
    # uses a negative look-behind to only split on separator characters
    # that are NOT preceded by an escape character (the backslash)
    pattern = re.compile(r'(?<!\\)' + re.escape(separator))
    values = pattern.split(string)
    # remove the escape character
    return [re.sub(r'\\(.)', r'\1', v) for v in values]


def join_values(values: List[Union[list, str]]) -> str:
    """Join either a list of strings, or a list of lists of strings. A list of
    strings will be separated with "|". A list of lists will be separated by ";".

    A list of strings represents a single property of a single subject that has
    multiple values. A list of lists represents a several instances of the same
    property, each belonging to a different subject. This latter case is used for
    embedded objects.

    ```pycon
    >>> join_values(['foo', 'bar'])
    'foo|bar'

    >>> join_values([['foo', 'bar'], ['delta']])
    'foo|bar;delta'
    ```
    """
    if values is None or len(values) == 0:
        return ''
    elif isinstance(values[0], list):
        return ';'.join(join_values(v) for v in values)
    else:
        return '|'.join(str(v) for v in values)


def build_lookup_index(index_string: str) -> Dict[str, Dict[int, str]]:
    """Build a lookup dictionary for embedded object properties of an item.

    From this `index_string`:

    ```python
    'author[0]=#alpha;author[1]=#beta;subject[0]=#delta'
    ```

    Returns this dictionary:

    ```python
    {
        'author': {
            0: 'alpha',
            1: 'beta',
        },
        'subject': {
            0: 'delta',
        },
    }
    ```
    """
    index = defaultdict(dict)
    if index_string is None or index_string == '':
        return index

    pattern = re.compile(r'(\w+)\[(\d+)]')
    for entry in index_string.split(';'):
        key, fragment = entry.split('=')
        m = pattern.search(key)
        attr = m[1]
        i = int(m[2])
        index[attr][i] = fragment.lstrip('#')
    return index


def flatten_headers(header_map: Dict[str, Union[str, dict]], prefix='') -> Dict[str, str]:
    """Transform a possibly nested mapping of attribute name to header label into a
    flat mapping of header label to attribute name. Nesting of the attribute names is
    indicated by a ".".

    ```pycon
    >>> header_mapping = {
    ...     'title': 'Title',
    ...     'creator': {
    ...         'label': 'Creator',
    ...         'same_as': 'Creator URI',
    ...     },
    ... }
    >>> flatten_headers(header_mapping)
    {'Title': 'title', 'Creator': 'creator.label', 'Creator URI': 'creator.same_as'}
    ```
    """
    headers = {}
    for attr, header in header_map.items():
        if isinstance(header, dict):
            headers.update(flatten_headers(header, prefix + attr + '.'))
        else:
            headers[header] = prefix + attr
    return headers


def flatten(description: RDFResourceBase, header_map: Dict[str, Union[str, dict]]) -> Dict[str, List[str]]:
    """Convert an RDF description to a dictionary with string header keys and list values.
    RDF attributes of the description object are mapped to the header keys by the `header_map`."""
    columns = defaultdict(list)
    for attr, header in header_map.items():
        if isinstance(header, dict):
            # treat this as an embedded ObjectProperty
            base_prop = getattr(description, attr)
            assert isinstance(base_prop, RDFObjectProperty), f'"{attr}" must be an object property'
            for n, obj in enumerate(base_prop.objects):
                # add this embedded object to the index for this row
                columns['INDEX'].append([f'{attr}[{n}]=#{URLObject(obj.uri).fragment}'])
                for key, values in flatten(obj, header).items():
                    columns[key].append(values)
        else:
            if description is not None:
                prop = getattr(description, attr)
                if isinstance(prop, RDFDataProperty):
                    # handle language tags on literals
                    languages = set(v.language for v in prop.values)
                    language_headers = {lang: header + (f' [{lang}]' if lang else '') for lang in languages}
                    for tag, tagged_header in language_headers.items():
                        columns[tagged_header] = [v for v in prop.values if v.language == tag]
                else:
                    columns[header] = list(prop.values)
            else:
                columns[header] = []
    return columns


class ColumnHeader(NamedTuple):
    """A column header with an optional language."""
    label: str
    """Column header"""
    language: str = None
    """Language (may be `None`)"""

    @classmethod
    def from_string(cls, header: str) -> 'ColumnHeader':
        """Parse the given string to get the label and language.

        ```pycon
        >>> ColumnHeader.from_string('Title [en]')
        ColumnHeader(label='Title', language='en')

        >>> ColumnHeader.from_string('Author')
        ColumnHeader(label='Author', language=None)
        ```
        """
        m = re.match(r'^([^[]*)(?: \[(.*)])?$', header)
        return cls(label=m[1], language=m[2])

    def __str__(self):
        return f'{self.label} [{self.language}]' if self.language is not None else self.label


def get_column_headers(headers: Iterable[str], base_header: str) -> List[ColumnHeader]:
    """From `headers`, return a column `ColumnHeader` for each header whose label is `base_header`.

    ```pycon
    >>> get_column_headers(['Title [en]', 'Title [de]', 'Author'], 'Title')
    [ColumnHeader(label='Title', language='en'), ColumnHeader(label='Title', language='de')]

    >>> get_column_headers(['Title [en]', 'Title [de]', 'Author'], 'Author')
    [ColumnHeader(label='Author', language=None)]

    >>> get_column_headers(['Title [en]', 'Title [de]', 'Author'], 'Date')
    []
    ```
    """
    return [
        ColumnHeader.from_string(h)
        for h in headers
        if h == base_header or re.match(base_header + r' \[.*]$', h)
    ]


def get_embedded_params(row: Mapping[str, str], header_labels: Iterable[str]) -> List[Dict[str, str]]:
    """From a data row and a set of header labels, construct a list of parameter
    dictionaries for one or more parallel embedded objects.

    For example, for this CSV:

    ```csv
    Title,Subject,Subject URI
    Foo,Linguistics;Philosophy,http://example.com/term/ling;http://example.com/term/phil
    ```

    Supplying the first row to this function, with `header_labels=["Subject", "Subject URI"]`,
    will yield this list:

    ```python
    [
        {
            "Subject": "Linguistics",
            "Subject URI": "http://example.com/ling",
        },
        {
            "Subject": "Philosophy",
            "Subject URI": "http://example.com/phil",
        },
    ]
    ```

    Which is suitable for passing to `unflatten()` for building the embedded objects themselves.
    """
    params = defaultdict(list)
    for header in header_labels:
        for column_header in get_column_headers(row.keys(), header):
            params[str(column_header)].extend(row[str(column_header)].split(';'))

    sub_rows = list(zip_longest(*params.values()))
    return [dict(zip(params.keys(), x)) for x in sub_rows]


def unflatten(
        row_data: Mapping[str, str],
        resource_class: Type[RDFResourceBase],
        header_map: Mapping[str, Union[str, dict]],
        index: Mapping[str, Mapping[int, str]] = None,
) -> Dict[str, List[Union[Literal, URIRef, EmbeddedObject]]]:
    """Transform a mapping of column headers to values (such as would be returned by a
    `csv.DictReader`) into a dictionary of parameters that can be passed to the constructor
    of an RDF description class to create an RDF description object."""
    if index is None:
        index = {}
    params = defaultdict(list)
    for attr, header in header_map.items():
        descriptor = getattr(resource_class, attr)
        if isinstance(header, dict):
            for n, sub_row in enumerate(get_embedded_params(row_data, header_labels=header.values())):
                embedded_params = unflatten(sub_row, descriptor.object_class, header, index)
                if any(embedded_params.values()):
                    params[attr].append(EmbeddedObject(
                        cls=descriptor.object_class,
                        fragment_id=index.get(attr, {}).get(n, None),
                        **embedded_params,
                    ))
        else:
            for column_header in get_column_headers(row_data.keys(), header):
                values = filter(not_empty, split_escaped(row_data.get(str(column_header)), separator='|'))
                if isinstance(descriptor, ObjectProperty):
                    params[attr].extend(URIRef(v) for v in values)
                else:
                    params[attr].extend(Literal(v, lang=column_header.language) for v in values)
    return params


@contextmanager
def ensure_text_mode(file):
    if 'b' in file.mode:
        # re-open in text mode
        fh = open(file.fileno(), mode=file.mode.replace('b', ''), closefd=False)
        yield fh
        fh.close()
    else:
        # file is already in text mode
        yield file


@contextmanager
def ensure_binary_mode(file):
    if 'b' not in file.mode:
        # re-open in binary mode
        fh = open(file.fileno(), mode=file.mode + 'b', closefd=False)
        yield fh
        fh.close()
    else:
        # file is already in binary mode
        yield file


def write_csv_file(row_info, file):
    # sort and add the new headers that have language names or datatypes
    for header, new_headers in row_info['extra_headers'].items():
        header_index = row_info['headers'].index(header)
        for i, new_header in enumerate(sorted(new_headers), start=1):
            row_info['headers'].insert(header_index + i, new_header)

    # strip out headers that aren't used in any row
    for header in row_info['headers']:
        has_column_values = any([True for row in row_info['rows'] if header in row])
        if not has_column_values:
            row_info['headers'].remove(header)

    # write the CSV file;
    # file must be opened in text mode, otherwise csv complains
    # about wanting str and not bytes
    with ensure_text_mode(file) as csv_file:
        csv_writer = csv.DictWriter(csv_file, row_info['headers'], extrasaction='ignore')
        csv_writer.writeheader()
        for row in row_info['rows']:
            csv_writer.writerow(row)


class CSVSerializer:
    """Serializer that encodes metadata records with a defined content model as CSV files."""

    SYSTEM_HEADERS = [
        'URI', 'PUBLIC URI', 'CREATED', 'MODIFIED', 'INDEX', 'FILES', 'ITEM_FILES', 'PUBLISH', 'HIDDEN'
    ]

    def __init__(self, directory: Union[str, Path] = None, public_uri_template: str = None):
        self.directory = Path(directory) or Path.cwd()
        """Destination directory for the CSV file(s)"""

        self.rows_by_content_model = {}
        """Internal accumulator of row data"""

        self.content_type = 'text/csv'
        self.file_extension = '.csv'
        self.public_uri_template = public_uri_template

    def __enter__(self):
        self.rows = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()

    def write(self, resource: RDFResourceBase, files: Iterable[BinaryResource] = None, binaries_dir=''):
        """
        Serializes the given resource as a CSV row using the `flatten()` function. The resulting row is
        added to an internal accumulator. The CSV file or files themselves are not actually written until
        the `finish()` method is called.
        """
        resource_class = type(resource)
        if resource_class not in self.rows_by_content_model:
            self.rows_by_content_model[resource_class] = {
                'header_map': resource_class.HEADER_MAP,
                'headers': list(flatten_headers(resource_class.HEADER_MAP).keys()) + self.SYSTEM_HEADERS,
                'extra_headers': defaultdict(set),
                'rows': []
            }

        graph = resource.graph
        row = {k: join_values(v) for k, v in flatten(resource, resource_class.HEADER_MAP).items()}
        row['URI'] = str(resource.uri)
        if files is not None:
            row['FILES'] = ';'.join(
                os.path.join(binaries_dir, file.describe(PCDMFile).filename.value) for file in files)
        row['CREATED'] = str(graph.value(resource.uri, fedora.created))
        row['MODIFIED'] = str(graph.value(resource.uri, fedora.lastModified))
        if self.public_uri_template is not None:
            # TODO: more generic transform function than hardwired to look for a uuid
            uri = urlparse(resource.uri)
            uuid = os.path.basename(uri.path)
            row['PUBLIC URI'] = self.public_uri_template.format(uuid=uuid)

        self.rows_by_content_model[resource_class]['rows'].append(row)

    LANGUAGE_NAMES = {
        'ja': 'Japanese',
        'ja-latn': 'Japanese (Romanized)'
    }
    LANGUAGE_CODES = {name: code for code, name in LANGUAGE_NAMES.items()}

    DATATYPE_NAMES = {
        URIRef('http://id.loc.gov/datatypes/edtf'): 'EDTF',
        URIRef('http://www.w3.org/2001/XMLSchema#date'): 'Date'
    }
    DATATYPE_URIS = {name: uri for uri, name in DATATYPE_NAMES.items()}

    def finish(self):
        """
        Writes the actual CSV file(s). For each detected content model, this will write
        a CSV file with the name `{ModelName}_metadata.csv` to the directory `directory`.

        Raises an `EmptyItemListError` if there are no items to export.
        """
        if len(self.rows_by_content_model) == 0:
            raise EmptyItemListError()

        for resource_class, row_info in self.rows_by_content_model.items():
            metadata_filename = resource_class.__name__ + '_metadata.csv'
            with (self.directory / metadata_filename).open(mode='w') as metadata_file:
                # write a CSV file for this model
                write_csv_file(row_info, metadata_file)


class EmptyItemListError(Exception):
    pass
