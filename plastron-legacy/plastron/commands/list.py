import logging

from plastron.commands import BaseCommand
from plastron.util import get_title_string, ResourceList, parse_predicate_list

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
        '-R', '--recursive',
        help='List additional objects found by traversing the given predicate(s)',
        action='store'
    )
    parser.add_argument(
        'uris', nargs='*',
        help='URIs of repository objects to list'
    )
    parser.set_defaults(cmd_name='list')


class Command(BaseCommand):
    def __call__(self, fcrepo, args):
        self.long = args.long

        resources = ResourceList(
            repository=fcrepo,
            uri_list=args.uris
        )

        resources.process(
            method=self.list_item,
            traverse=parse_predicate_list(args.recursive),
            use_transaction=False
        )

    def list_item(self, resource, graph):
        if self.long:
            title = get_title_string(graph)
            print(f'{resource} {title}')
        else:
            print(resource)
