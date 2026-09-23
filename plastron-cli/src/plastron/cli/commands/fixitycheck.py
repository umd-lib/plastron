import csv
import json
import logging
from collections.abc import Callable, Iterable

import sys
from argparse import Namespace, FileType

from plastron.cli import get_uris
from plastron.cli.commands import BaseCommand
from plastron.files import BinaryResource, FixityCheckingError
from plastron.models.fedora import FedoraBinary

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='fixitycheck',
        description='Run a fixity check on objects in the repository',
    )
    parser.add_argument(
        '-f', '--uris-file', action='store', type=FileType(), help='file containing URIs of objects to fixity check'
    )
    parser.add_argument(
        '--format',
        '-t',
        choices=['csv', 'jsonl'],
        default='csv',
        help='output format of results; default is "csv"',
    )
    parser.add_argument(
        'uris',
        nargs='*',
        metavar='URI',
        help='URIs of objects to fixity check',
    )
    parser.set_defaults(cmd_name='fixitycheck')


def get_writer(output_format: str) -> Callable[[dict], None]:
    match output_format:
        case 'csv':
            writer = csv.writer(sys.stdout)
            return lambda result: writer.writerow(result.values())
        case 'jsonl':
            return lambda result: print(json.dumps(result))
        case _:
            raise RuntimeError(f'Unknown output format: {output_format}')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        # mimicking a click.Context object to bridge between argparse and click commands
        ctx = Namespace(obj=self.context)
        fixitycheck(ctx, uris=get_uris(args), output_format=args.format)


def fixitycheck(ctx: Namespace, uris: Iterable[str], output_format: str):
    write = get_writer(output_format)
    for uri in uris:
        binary_resource = ctx.obj.repo.read(uri, BinaryResource)

        try:
            fixity_details = binary_resource.check_fixity()
        except FixityCheckingError as e:
            logger.error(f'Skipping {uri}: {e}')
            continue

        if fixity_details.is_success:
            logger.info(f'Fixity check of {uri} succeeded')
        else:
            logger.info(f'Fixity check of {uri} failed: {fixity_details.outcome}')

        obj = binary_resource.describe(FedoraBinary)
        result = {
            'uri': str(obj.uri),
            'outcome': ';'.join(fixity_details.outcome),
            'checksum': str(fixity_details.digest),
            'size': int(str(fixity_details.size)),
            'time': fixity_details.timestamp.isoformat(),
            'expected_checksum': str(obj.digest),
            'expected_size': int(str(obj.size)),
            'last_modified': str(obj.last_modified),
        }
        write(result)
