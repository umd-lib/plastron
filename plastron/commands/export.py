import logging
import csv
import tempfile
import numpy as np

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='export',
        description='Export resources from the repository'
    )
    parser.add_argument(
        '-n', '--name',
        help='Export job name',
        action='store',
        required=True
    )
    parser.add_argument(
        '-f', '--format',
        help='Export job format',
        action='store',
        choices=['csv', 'turtle'],
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


def csv_serialize(graph, headers, csvwriter):
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
        subject_row = [None] * len(headers)
        subject_row[0] = s
        subject_rows[s] = [subject_row, used_headers]

    for (s, p, o) in graph.triples((None, None, None)):
        subject_row, used_headers = subject_rows[s]
        used_headers[p] = 1 if p not in used_headers else used_headers[p] + 1
        # Create a new header for the predicate, if missing or need to duplicate predicate header more times
        if (p not in headers) or (headers.count(p) < used_headers[p]):
            headers.append(p)
        predicate_index = get_nth_index(headers, p, used_headers[p])
        if len(subject_row) <= predicate_index:
            subject_row.extend([None] * ((predicate_index + 1) - len(subject_row)))
        subject_row[predicate_index] = o

    for subject in sorted(list(subjects)):
        csvwriter.writerow(subject_rows[subject][0])


def write_csv_with_header(csvwriter, headers, datarows_file):
    """
    Writes the provided headers and the datarows from datarows_file with the given csvwriter
    - Sorts the headers alphabettically
    - Sorts the datarows to match the sorted headers order
    """
    np_headers = np.array(headers)
    sort_order = np.argsort(np_headers)
    sorted_headers = np_headers[sort_order].tolist()
    csvwriter.writerow(sorted_headers)
    datarows_file.flush()
    datarows_file.seek(0)
    for row in csv.reader(datarows_file):
        row.extend([None] * (len(headers) - len(row)))
        np_row = np.array(row)
        sorted_row = np_row[sort_order].tolist()
        csvwriter.writerow(sorted_row)


class Command:
    def __call__(self, fcrepo, args):
        count = 0
        total = len(args.uris)
        format = args.format if args.format == 'csv' else 'ttl'
        mode = 'w' if args.format == 'csv' else 'wb'

        with open(f'export-{args.name}.{format}', mode) as fh, tempfile.TemporaryFile(mode='w+') as tmp_datarows_file:
            csvwriter = csv.writer(fh)
            csv_datarow_writer = csv.writer(tmp_datarows_file)
            csv_headers = ['Subject']
            for uri in args.uris:
                r = fcrepo.head(uri)
                if r.status_code == 200:
                    # do export
                    logger.info(f'Exporting item {count + 1}/{total}: {uri}')
                    graph = fcrepo.get_graph(uri)
                    if format == 'csv':
                        csv_serialize(graph, csv_headers, csv_datarow_writer)
                    else:
                        graph.serialize(destination=fh, format='turtle')
                    count += 1
            if format == 'csv':
                write_csv_with_header(csvwriter, csv_headers, tmp_datarows_file)

        logger.info(f'Exported {count} of {total} items')
