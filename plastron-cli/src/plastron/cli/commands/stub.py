import csv
import logging
import sys
from argparse import FileType, Namespace
from typing import Optional

from rdflib import URIRef

from plastron.cli.commands import BaseCommand
from plastron.client import ClientError
from plastron.files import BinarySource, HTTPFileSource, LocalFileSource
from plastron.models.umd import Stub
from plastron.utils import uri_or_curie
from plastron.repo import ContainerResource
from plastron.repo.pcdm import PCDMFileBearingResource
from plastron.repo.utils import context

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
            'column in the source CSV file with the location of the binary to '
            'load. Supports http: and https: (must begin with "http:" or '
            '"https:"), and file resources (relative or absolute file path). '
            'Relative file paths are relative to where the command is run.'
        ),
        required=True,
        action='store'
    )
    parser.add_argument(
        '--rename-binary-column',
        help=(
            'Renames the binary column in the CSV output to '
            'the provided name.'
        ),
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


def get_source(binary_column_value: str) -> Optional[BinarySource]:
    """
    Returns the appropriate BinarySource implementation to use, based on the
    value in the binary column, or None if an appropriate BinarySource
    implementation cannot be determined.
    """
    source: Optional[BinarySource] = None
    if binary_column_value.startswith("http:") or binary_column_value.startswith("https:"):
        source = HTTPFileSource(binary_column_value)
    elif binary_column_value is not None:
        source = LocalFileSource(binary_column_value)
    return source


def write_csv_header(csv_file: csv.DictReader, args: Namespace, csv_writer: csv.DictWriter) -> None:
    """
    Writes the CSV header line to the output, possibly replacing the
    binary_column header with a renamed header.

    This is needed because the binary_column in the output will always
    be a URL, while the binary_column in the input may be a filepath
    (and thus have a column name that is not descriptive of the output).
    """
    if csv_file.fieldnames is not None:
        csv_header_dict = dict(zip(csv_file.fieldnames, csv_file.fieldnames))
        if args.rename_binary_column is not None:
            csv_header_dict[args.binary_column] = args.rename_binary_column
        csv_writer.writerow(csv_header_dict)


class Command(BaseCommand):
    def __call__(self, args: Namespace) -> None:
        csv_file = csv.DictReader(args.source_file)
        if csv_file.fieldnames is None:
            logger.error(f'No fields found in {csv_file}. Exiting.')
            sys.exit(1)

        if args.output_file is not None:
            output_file = open(args.output_file, 'w')
        else:
            output_file = sys.stdout
        csv_writer = csv.DictWriter(output_file, fieldnames=csv_file.fieldnames)

        write_csv_header(csv_file, args, csv_writer)
        container_path = args.container_path or self.context.repo.endpoint.relpath
        container_resource: ContainerResource = self.context.repo[container_path:ContainerResource].read()

        for _, row in enumerate(csv_file, start=1):
            identifier = row[args.identifier_column]
            source = get_source(row[args.binary_column])
            if not source:
                logger.warning(f'No source found for {identifier}; skipping')
                csv_writer.writerow(row)
                continue

            item = Stub(title=f'Stub for {identifier}', identifier=identifier)

            if args.member_of is not None:
                item.member_of = URIRef(args.member_of)
            if args.access is not None:
                item.rdf_type.add(args.access)

            with context(repo=self.context.repo):
                try:
                    access_types = [args.access] if args.access is not None else []

                    resource = container_resource.create_child(
                        resource_class=PCDMFileBearingResource,
                        description=item
                    )
                    resource.create_file(source, rdf_types=access_types)

                    row[args.binary_column] = resource.url
                    csv_writer.writerow(row)
                except (ClientError, FileNotFoundError) as e:
                    logger.error(f'{item.identifier} not created: {e}')
                else:
                    logger.info(f'Created {resource.url}')

        if output_file is not sys.stdout:
            output_file.close()
