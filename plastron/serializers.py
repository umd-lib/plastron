import csv
import logging
import os
from collections import defaultdict
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse
from zipfile import ZipFile

from rdflib import Literal, Graph

from plastron.exceptions import DataReadException
from plastron.models.letter import Letter
from plastron.models.poster import Poster
from plastron.namespaces import get_manager, bibo, rdf, fedora
from plastron.rdf import RDFObjectProperty, RDFDataProperty, Resource

logger = logging.getLogger(__name__)
nsm = get_manager()

MODEL_MAP = {
    bibo.Image: Poster,
    bibo.Letter: Letter
}


class TurtleSerializer:
    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.content_type = 'text/turtle'
        self.file_extension = '.ttl'

    def __enter__(self):
        self.fh = open(self.filename, 'wb')
        return self

    def write(self, graph):
        graph.serialize(destination=self.fh, format='turtle')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fh.close()


def detect_resource_class(graph, subject):
    types = set([o for s, p, o in graph.triples((subject, rdf.type, None))])

    for rdf_type, cls in MODEL_MAP.items():
        if rdf_type in types:
            return cls
    else:
        raise DataReadException(f'Unable to detect resource type for {subject}')


def write_csv_file(row_info, file):
    # sort and add the new headers that have language names
    for header, new_headers in row_info['language_headers'].items():
        header_index = row_info['headers'].index(header)
        for i, new_header in enumerate(sorted(new_headers), start=1):
            row_info['headers'].insert(header_index + i, new_header)

    # strip out headers that aren't used in any row
    for header in row_info['headers']:
        has_column_values = any([True for row in row_info['rows'] if header in row])
        if not has_column_values:
            row_info['headers'].remove(header)

    # write the CSV file
    csv_writer = csv.DictWriter(file, row_info['headers'], extrasaction='ignore')
    csv_writer.writeheader()
    for row in row_info['rows']:
        csv_writer.writerow(row)


class CSVSerializer:
    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.content_models = {}
        self.content_type = 'text/csv'
        self.file_extension = '.csv'
        self.public_uri_template = kwargs.get('public_uri_template', None)

    def __enter__(self):
        self.rows = []
        return self

    SYSTEM_HEADERS = ['URI', 'PUBLIC URI', 'CREATED', 'MODIFIED', 'INDEX']

    def write(self, graph: Graph):
        """
        Serializes the given graph as CSV data rows.
          - One row per subject, if there are multiple subjects (HashURIs)
          - The data rows written to the csv writer with primary subject row first, followed by HashURI subject rows
          - Appends new predicates if missing or for repeating values of the predicate to the provided headers object.
        """
        main_subject = set([s for s in graph.subjects() if '#' not in str(s)]).pop()
        resource_class = detect_resource_class(graph, main_subject)
        if resource_class not in self.content_models:
            self.content_models[resource_class] = {
                'header_map': resource_class.HEADER_MAP,
                'headers': list(resource_class.HEADER_MAP.values()) + self.SYSTEM_HEADERS,
                'language_headers': defaultdict(set),
                'rows': []
            }

        resource = resource_class.from_graph(graph, subject=main_subject)
        row = {k: ';'.join(v) for k, v in self.flatten(resource, self.content_models[resource_class]).items()}
        row['URI'] = str(main_subject)
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

                # create additional columns (if needed) for different languages
                if isinstance(prop, RDFDataProperty):
                    per_language_columns = defaultdict(list)
                    for value in prop.values:
                        if isinstance(value, Literal) and value.language:
                            language_code = value.language
                        else:
                            language_code = ''
                        per_language_columns[language_code].append(value)
                    for language_code, values in per_language_columns.items():
                        serialization = '|'.join(values)
                        if language_code:
                            language = self.LANGUAGE_NAMES[language_code]
                            language_header = f'{header} [{language}]'
                            row_info['language_headers'][header].add(language_header)
                            columns[language_header].append(serialization)
                        else:
                            columns[header].append(serialization)
                else:
                    serialization = '|'.join(prop.values)
                    columns[header].append(serialization)
        return columns

    def __exit__(self, exc_type, exc_val, exc_tb):
        if len(self.content_models) == 0:
            logger.error("No items could be exported; skipping writing file")
        elif len(self.content_models) == 1:
            # write a single CSV file
            with open(self.filename, mode='w') as fh:
                write_csv_file(next(iter(self.content_models.values())), file=fh)
        else:
            # write a ZIP file containing individual CSV files
            with ZipFile(self.filename, mode='w') as zip_fh:
                for resource_class, row_info in self.content_models.items():
                    # write the CSV file
                    tmp = NamedTemporaryFile(mode='w', encoding='utf-8', delete=False)
                    write_csv_file(row_info, file=tmp)
                    tmp.close()
                    # add the CSV file to the ZIP file
                    zip_fh.write(tmp.name, arcname=resource_class.__name__ + '.csv')
                    os.remove(tmp.name)
            # multi-content model CSV export actually produces ZIP files
            self.content_type = 'application/zip'
            self.file_extension = '.zip'


SERIALIZER_CLASSES = {
    'text/turtle': TurtleSerializer,
    'turtle': TurtleSerializer,
    'ttl': TurtleSerializer,
    'text/csv': CSVSerializer,
    'csv': CSVSerializer
}
