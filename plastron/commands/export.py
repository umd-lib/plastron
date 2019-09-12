import logging
import csv
import os
import tempfile
import numpy as np
from rdflib import Literal
from plastron.namespaces import get_manager
from plastron.exceptions import ConfigException

logger = logging.getLogger(__name__)
nsm = get_manager()

def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='export',
        description='Export resources from the repository'
    )
    parser.add_argument(
        '-o', '--output-file',
        help='File to write export package to',
        action='store',
    )
    parser.add_argument(
        '-f', '--format',
        help='Export job format',
        action='store',
        choices=Command.SERIALIZER_CLASS_FOR.keys(),
        required=True
    )
    parser.add_argument(
        'uris',
        nargs='*',
        help='URIs of repository objects to export'
    )
    parser.set_defaults(cmd_name='export')


def get_nth_index(list, item, n):
    """
    Returns the Nth index of the specified item in the provided list
    """
    return [index for index, _item in enumerate(list) if _item == item][n - 1]


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


class CSVSerializer:
    FILE_EXTENSION = 'csv'

    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.datarows_file = tempfile.TemporaryFile(mode='w+')
        self.datarows_writer = csv.writer(self.datarows_file)
        self.headers = ['Subject']
        return self

    def write(self, graph):
        """
        Serializes the given graph as CSV data rows.
          - One row per subject, if there are multiple subjects (HashURIs)
          - The data rows written to the csv writer with primary subject row first, followed by HashURI subject rows
          - Appends new predicates if missing or for repeating values of the predicate to the provided headers object.
        """
        subject_rows = {}
        subjects = set(graph.subjects())
        for s in subjects:
            used_headers = {}  # To track the number times a predicate repeats for the given subject
            subject_row = [None] * len(self.headers)
            subject_row[0] = s
            subject_rows[s] = [subject_row, used_headers]

        for (s, p, o) in graph.triples((None, None, None)):
            p = p.n3(namespace_manager=nsm)
            if isinstance(o, Literal):
                if o.language is not None:
                    p = f'{p}@{o.language}'
                if o.datatype is not None:
                    p = f'{p}^^{o.datatype.n3(namespace_manager=nsm)}'
            subject_row, used_headers = subject_rows[s]
            used_headers[p] = 1 if p not in used_headers else used_headers[p] + 1
            # Create a new header for the predicate, if missing or need to duplicate predicate header more times
            if (p not in self.headers) or (self.headers.count(p) < used_headers[p]):
                self.headers.append(p)
            predicate_index = get_nth_index(self.headers, p, used_headers[p])
            if len(subject_row) <= predicate_index:
                subject_row.extend([None] * ((predicate_index + 1) - len(subject_row)))
            subject_row[predicate_index] = o

        for subject in sorted(list(subjects)):
            self.datarows_writer.writerow(subject_rows[subject][0])

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.write_csv_with_header()
        self.datarows_file.close()

    def write_csv_with_header(self):
        """
        Writes the provided headers and the datarows from datarows_file with the given csvwriter
        - Sorts the headers alphabetically
        - Sorts the datarows to match the sorted headers order
        """
        with open(self.filename, 'w') as fh:
            writer = csv.writer(fh)
            np_headers = np.array(self.headers)
            sort_order = np.argsort(np_headers)
            sorted_headers = np_headers[sort_order].tolist()
            writer.writerow(sorted_headers)
            self.datarows_file.flush()
            self.datarows_file.seek(0)
            for row in csv.reader(self.datarows_file):
                row.extend([None] * (len(self.headers) - len(row)))
                np_row = np.array(row)
                sorted_row = np_row[sort_order].tolist()
                writer.writerow(sorted_row)


class Command:
    SERIALIZER_CLASS_FOR = {
        'text/turtle': TurtleSerializer,
        'turtle': TurtleSerializer,
        'ttl': TurtleSerializer,
        'text/csv': CSVSerializer,
        'csv': CSVSerializer
    }

    def __call__(self, fcrepo, args):
        count = 0
        total = len(args.uris)
        try:
            serializer_class = self.SERIALIZER_CLASS_FOR[args.format]
        except KeyError:
            raise ConfigException(f'Unknown format: {args.format}')

        logger.debug(f'Exporting to file {args.output_file}')
        with serializer_class(args.output_file) as serializer:
            for uri in args.uris:
                r = fcrepo.head(uri)
                if r.status_code == 200:
                    # do export
                    if 'describedby' in r.links:
                        # the resource is a binary, get the RDF description URI
                        rdf_uri = r.links['describedby']['url']
                    else:
                        rdf_uri = uri
                    logger.info(f'Exporting item {count + 1}/{total}: {uri}')
                    graph = fcrepo.get_graph(rdf_uri)
                    serializer.write(graph)
                    count += 1

        logger.info(f'Exported {count} of {total} items')
