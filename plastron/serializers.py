import csv
import logging
from collections import defaultdict
from urllib.parse import urlparse

from rdflib import Literal

from plastron.exceptions import DataReadException
from plastron.models.letter import Letter
from plastron.models.poster import Poster
from plastron.namespaces import get_manager, bibo, rdf, fedora
from plastron.rdf import RDFObjectProperty, RDFDataProperty

logger = logging.getLogger(__name__)
nsm = get_manager()

MODEL_MAP = {
    bibo.Image: Poster,
    bibo.Letter: Letter
}


class TurtleSerializer:
    FILE_EXTENSION = 'ttl'

    def __init__(self, filename):
        self.filename = filename

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


class CSVSerializer:
    FILE_EXTENSION = 'csv'

    def __init__(self, filename):
        self.filename = filename
        self.resource_class = None
        self.header_map = None
        self.headers = None

    def __enter__(self):
        self.rows = []
        return self

    def write(self, graph):
        """
        Serializes the given graph as CSV data rows.
          - One row per subject, if there are multiple subjects (HashURIs)
          - The data rows written to the csv writer with primary subject row first, followed by HashURI subject rows
          - Appends new predicates if missing or for repeating values of the predicate to the provided headers object.
        """
        main_subject = set([s for s in graph.subjects() if '#' not in str(s)]).pop()
        if self.resource_class is None:
            self.resource_class = detect_resource_class(graph, main_subject)

        self.header_map = self.resource_class.HEADER_MAP
        self.headers = list(self.header_map.values()) + ['URI', 'CREATED', 'MODIFIED', 'INDEX']

        resource = self.resource_class.from_graph(graph, subject=main_subject)
        row = {k: ';'.join(v) for k, v in self.flatten(resource).items()}
        row['URI'] = str(main_subject)
        row['CREATED'] = str(graph.value(main_subject, fedora.created))
        row['MODIFIED'] = str(graph.value(main_subject, fedora.lastModified))
        self.rows.append(row)

    LANGUAGE_NAMES = {
        'ja': 'Japanese',
        'ja-latn': 'Japanese (Romanized)'
    }

    def flatten(self, resource, prefix=''):
        columns = defaultdict(lambda: [])
        for name, prop in resource.props.items():
            if isinstance(prop, RDFObjectProperty) and prop.is_embedded:
                for i, obj in enumerate(prop.values):
                    # record the list position to hash URI correlation
                    columns['INDEX'].append(f'{name}[{i}]=#{urlparse(obj.uri).fragment}')
                    for header, value in self.flatten(obj, prefix=f'{name}.').items():
                        columns[header].extend(value)
            else:
                key = prefix + name
                if key not in self.header_map:
                    continue
                header = self.header_map[key]

                # create additional columns (if needed) for different languages
                if isinstance(prop, RDFDataProperty):
                    header_index = self.headers.index(header)
                    per_language_columns = defaultdict(lambda: [])
                    for value in prop.values:
                        if isinstance(value, Literal) and value.language:
                            language_code = value.language
                        else:
                            language_code = ''
                        per_language_columns[language_code].append(value)
                    new_headers = []
                    for language_code, values in per_language_columns.items():
                        serialization = '|'.join(values)
                        if language_code:
                            language = self.LANGUAGE_NAMES[language_code]
                            language_header = f'{header} [{language}]'
                            new_headers.append(language_header)
                            columns[language_header].append(serialization)
                        else:
                            columns[header].append(serialization)
                    # sort and add the new headers that have language names
                    for i, new_header in enumerate(sorted(new_headers), start=1):
                        self.headers.insert(header_index + i, new_header)
                else:
                    serialization = '|'.join(prop.values)
                    columns[header].append(serialization)
        return columns

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.headers is None:
            logger.error("No items could be exported; skipping writing file")
            return
        # strip out headers that aren't used in any row
        for header in self.headers:
            has_column_values = any([True for row in self.rows if header in row])
            if not has_column_values:
                self.headers.remove(header)
        # write the CSV file
        with open(self.filename, 'w') as fh:
            csv_writer = csv.DictWriter(fh, self.headers, extrasaction='ignore')
            csv_writer.writeheader()
            for row in self.rows:
                csv_writer.writerow(row)


SERIALIZER_CLASSES = {
    'text/turtle': TurtleSerializer,
    'turtle': TurtleSerializer,
    'ttl': TurtleSerializer,
    'text/csv': CSVSerializer,
    'csv': CSVSerializer
}
