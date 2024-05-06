import logging
from argparse import Namespace

from plastron.cli.commands import BaseCommand
from plastron.jobs.exportjob import ExportJob
from plastron.serializers import SERIALIZER_CLASSES

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='export',
        description='Export resources from the repository as a BagIt bag'
    )
    parser.add_argument(
        '-o', '--output-dest',
        help='Where to send the export. Can be a local filename or an SFTP URI',
        required=True,
        action='store'
    )
    parser.add_argument(
        '--key',
        help='SSH private key file to use for SFTP connections',
        action='store'
    )
    parser.add_argument(
        '-f', '--format',
        help='Format for exported metadata',
        action='store',
        choices=SERIALIZER_CLASSES.keys(),
        required=True
    )
    parser.add_argument(
        '--uri-template',
        help='Public URI template',
        action='store'
    )
    parser.add_argument(
        '-B', '--export-binaries',
        help='Export binaries in addition to the metadata',
        action='store_true'
    )
    parser.add_argument(
        '--binary-types',
        help='Include only binaries with a MIME type from this list',
        action='store'
    )
    parser.add_argument(
        'uris',
        nargs='*',
        help='URIs of repository objects to export'
    )
    parser.set_defaults(cmd_name='export')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        export_job = ExportJob(
            context=self.context,
            export_binaries=args.export_binaries,
            binary_types=args.binary_types,
            uris=args.uris,
            export_format=args.format,
            output_dest=args.output_dest,
            uri_template=args.uri_template,
            key=args.key,
        )
        self.run(export_job.run())
