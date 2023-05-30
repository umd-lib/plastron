import os

from exceptions import FailureException
from jobs import ItemLog
from plastron.commands import BaseCommand


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='verify',
        description="Verify item URI's are indexed in Solr and display URI's that aren't"
    )
    parser.add_argument(
        '-l', '--log',
        help='A completed log file from an import job',
        action='store'
    )

    parser.set_defaults(cmd_name='annotate')


class Command(BaseCommand):
    def __call__(self, args):
        if not os.path.isfile(args.log):
            raise FailureException('Path to log file is not valid')
        
        pass
