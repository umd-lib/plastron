import csv
import logging
import sys
from argparse import FileType

from plastron.commands import BaseCommand
from plastron.exceptions import FailureException, RESTAPIException
from plastron.files import HTTPFileSource
from plastron.http import Transaction
from plastron.models import Item
from plastron.pcdm import File
from plastron.util import uri_or_curie
from rdflib import URIRef


logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='stub',
        description='create stub resources with just an identifier and binary'
    )
    parser.add_argument(
        '--identifier-column',
        help='column in the source CSV file with a unique identifier for each item',
        required=True,
        action='store'
    )
    parser.add_argument(
        '--binary-column',
        help=(
            'column in the source CSV file with the URI of the binary to load; '
            'only supports http: and https: resources at this time'
        ),
        required=True,
        action='store'
    )
    parser.add_argument(
        '--member-of',
        help='URI of the object that new items are PCDM members of',
        action='store'
    )
    parser.add_argument(
        '--access',
        help='URI or CURIE of the access class to apply to new items',
        type=uri_or_curie,
        action='store'
    )
    parser.add_argument(
        '--container',
        help=(
            'parent container for new items; defaults to the RELPATH '
            'in the repo configuration file'
        ),
        dest='container_path',
        action='store'
    )
    parser.add_argument(
        '-o', '--output-file',
        help=(
            'destination for a copy of the source CSV file '
            'with the binary-column value replaced with the '
            'newly created repository URI for the binary; '
            'defaults to STDOUT if not given'
        ),
        action='store'
    )
    parser.add_argument(
        'source_file',
        help=(
            'name of the CSV file to create stubs from; '
            'use "-" to read from STDIN'
        ),
        type=FileType('r', encoding='utf-8-sig'),
        action='store'
    )
    parser.set_defaults(cmd_name='stub')


class Command(BaseCommand):
    def __call__(self, repo, args):
        csv_file = csv.DictReader(args.source_file)
        if args.output_file is not None:
            output_file = open(args.output_file, 'w')
        else:
            output_file = sys.stdout
        csv_writer = csv.DictWriter(output_file, fieldnames=csv_file.fieldnames)
        csv_writer.writeheader()
        for n, row in enumerate(csv_file, start=1):
            id = row[args.identifier_column]
            if not row[args.binary_column]:
                logger.warning(f'No binary source URI found for {id}; skipping')
                csv_writer.writerow(row)
                continue
            source = HTTPFileSource(row[args.binary_column])
            item = Item(identifier=id, title=f'Stub for {id}')
            file = File()
            file.source = source
            item.add_file(file)
            if args.member_of is not None:
                item.member_of = URIRef(args.member_of)
            if args.access is not None:
                item.rdf_types.add(args.access)
                file.rdf_types.add(args.access)
            try:
                with Transaction(repo) as txn:
                    try:
                        item.create(repo, container_path=args.container_path)
                        item.recursive_update(repo)
                        # update the CSV with the new URI
                        row[args.binary_column] = file.uri
                        csv_writer.writerow(row)
                        txn.commit()
                    except (RESTAPIException, FileNotFoundError) as e:
                        # if anything fails during item creation or committing the transaction
                        # attempt to rollback the current transaction
                        # failures here will be caught by the main loop's exception handler
                        # and should trigger a system exit
                        logger.error(f'{item.identifier} not created: {e}')
                        txn.rollback()
                    except KeyboardInterrupt:
                        logger.warning("Load interrupted")
                        txn.rollback()
                        raise

            except RESTAPIException as e:
                raise FailureException(f'Transaction rollback failed: {e}') from e

        if output_file is not sys.stdout:
            output_file.close()
