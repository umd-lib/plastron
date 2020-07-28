import csv
import logging
import os
from collections import defaultdict
from contextlib import contextmanager
from plastron.exceptions import DataReadException
from plastron.models import Issue, Letter, Poster
from plastron.namespaces import get_manager, bibo, rdf, fedora
from plastron.rdf import RDFObjectProperty, RDFDataProperty, Resource
from rdflib import Literal, Graph, URIRef
from urllib.parse import urlparse


logger = logging.getLogger(__name__)
nsm = get_manager()

MODEL_MAP = {
    bibo.Image: Poster,
    bibo.Issue: Issue,
    bibo.Letter: Letter
}


def detect_resource_class(graph, subject, fallback=None):
    types = set(graph.objects(URIRef(subject), rdf.type))

    for rdf_type, cls in MODEL_MAP.items():
        if rdf_type in types:
            return cls
    else:
        if fallback is not None:
            return fallback
        else:
            raise DataReadException(f'Unable to detect resource type for {subject}')


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


class TurtleSerializer:
    def __init__(self, directory, **_kwargs):
        self.directory_name = directory
        self.content_type = 'text/turtle'
        self.file_extension = '.ttl'

    def __enter__(self):
        return self

    def write(self, graph: Graph, **_kwargs):
        graph.namespace_manager = nsm
        with open(os.path.join(self.directory_name, 'metadata.ttl'), mode='wb') as export_file:
            graph.serialize(destination=export_file, format='turtle')

    def finish(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


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

    # write the CSV file
    # file must be opened in text mode, otherwise csv complains
    # about wanting str and not bytes
    with ensure_text_mode(file) as csv_file:
        csv_writer = csv.DictWriter(csv_file, row_info['headers'], extrasaction='ignore')
        csv_writer.writeheader()
        for row in row_info['rows']:
            csv_writer.writerow(row)


class CSVSerializer:
    SYSTEM_HEADERS = ['URI', 'PUBLIC URI', 'CREATED', 'MODIFIED', 'INDEX', 'FILES']

    def __init__(self, directory=None, public_uri_template=None):
        self.directory_name = directory or os.path.curdir
        self.content_models = {}
        self.content_type = 'text/csv'
        self.file_extension = '.csv'
        self.public_uri_template = public_uri_template

    def __enter__(self):
        self.rows = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()

    def write(self, graph: Graph, files=None, binaries_dir=''):
        """
        Serializes the given graph as CSV data rows.
        """
        main_subject = set([s for s in graph.subjects() if '#' not in str(s)]).pop()
        resource_class = detect_resource_class(graph, main_subject)
        if resource_class not in self.content_models:
            self.content_models[resource_class] = {
                'header_map': resource_class.HEADER_MAP,
                'headers': list(resource_class.HEADER_MAP.values()) + self.SYSTEM_HEADERS,
                'extra_headers': defaultdict(set),
                'rows': []
            }

        resource = resource_class.from_graph(graph, subject=main_subject)
        row = {k: ';'.join(v) for k, v in self.flatten(resource, self.content_models[resource_class]).items()}
        row['URI'] = str(main_subject)
        if files is not None:
            row['FILES'] = ';'.join(os.path.join(binaries_dir, file.filename[0]) for file in files)
        row['CREATED'] = str(graph.value(main_subject, fedora.created))
        row['MODIFIED'] = str(graph.value(main_subject, fedora.lastModified))
        if self.public_uri_template is not None:
            uri = urlparse(main_subject)
            uuid = os.path.basename(uri.path)
            row['PUBLIC URI'] = self.public_uri_template.format(uuid=uuid)

        self.content_models[resource_class]['rows'].append(row)

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

    def flatten(self, resource: Resource, row_info: dict, prefix=''):
        columns = defaultdict(list)
        for name, prop in resource.props.items():
            if isinstance(prop, RDFObjectProperty) and prop.is_embedded:
                for i, obj in enumerate(prop.values):
                    # record the list position to hash URI correlation
                    columns['INDEX'].append(f'{name}[{i}]=#{urlparse(obj.uri).fragment}')
                    for header, value in self.flatten(obj, row_info, prefix=f'{name}.').items():
                        columns[header].extend(value)
            else:
                key = prefix + name
                if key not in row_info['header_map']:
                    continue
                header = row_info['header_map'][key]

                # create additional columns (if needed) for different languages and datatypes
                if isinstance(prop, RDFDataProperty):

                    # ensure we only have literals here
                    literals = [v for v in prop.values if isinstance(v, Literal)]

                    languages = set(v.language for v in literals if v.language)
                    datatypes = set(v.datatype for v in literals if v.datatype)

                    for language in languages:
                        language_label = self.LANGUAGE_NAMES.get(language, language)
                        language_header = f'{header} [{language_label}]'
                        values = [v for v in prop.values if v.language == language]
                        serialization = '|'.join(values)
                        columns[language_header].append(serialization)
                        row_info['extra_headers'][header].add(language_header)
                    for datatype in datatypes:
                        datatype_label = self.DATATYPE_NAMES.get(datatype, datatype.n3(nsm))
                        datatype_header = f'{header} {{{datatype_label}}}'
                        values = [v for v in prop.values if v.datatype == datatype]
                        serialization = '|'.join(values)
                        columns[datatype_header].append(serialization)
                        row_info['extra_headers'][header].add(datatype_header)

                    # get the other values; literals without language or datatype, or URIRefs that snuck in
                    values = [v for v in prop.values
                              if not isinstance(v, Literal) or (not v.language and not v.datatype)]
                    if len(values) > 0:
                        columns[header].append('|'.join(values))
                else:
                    serialization = '|'.join(prop.values)
                    columns[header].append(serialization)

        return columns

    def finish(self):
        """
        Writes the actual CSV file(s)
        """
        if len(self.content_models) == 0:
            raise EmptyItemListError()

        for resource_class, row_info in self.content_models.items():
            metadata_filename = os.path.join(self.directory_name, resource_class.__name__ + '_metadata.csv')
            with open(metadata_filename, mode='w') as metadata_file:
                # write a CSV file for this model
                write_csv_file(row_info, metadata_file)


class EmptyItemListError(Exception):
    pass


SERIALIZER_CLASSES = {
    'text/turtle': TurtleSerializer,
    'turtle': TurtleSerializer,
    'ttl': TurtleSerializer,
    'text/csv': CSVSerializer,
    'csv': CSVSerializer
}
