import logging
from argparse import Namespace

from plastron.cli.commands import BaseCommand
from plastron.models.pcdm import PCDMFile
from plastron.namespaces import ldp

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='list',
        aliases=['ls'],
        description='List objects in the repository'
    )
    # long mode to print more than just the URIs (name modeled after ls -l)
    parser.add_argument(
        '-l', '--long',
        help='Display additional information besides the URI',
        action='store_true'
    )
    parser.add_argument(
        'uris', nargs='*',
        help='URIs of repository objects to list'
    )
    parser.set_defaults(cmd_name='list')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        self.long = args.long

        for uri in args.uris:
            resource = self.context.repo[uri].read()

            if resource.is_binary:
                print(uri)
                continue

            for child_resource in resource.walk(min_depth=1, max_depth=1, traverse=[ldp.contains]):
                if self.long:
                    description = child_resource.describe(PCDMFile)
                    title = str(description.title)
                    print(f'{child_resource.url} {title}')
                else:
                    print(child_resource.url)
