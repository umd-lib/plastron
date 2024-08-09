import logging
from argparse import Namespace

from plastron.cli import get_uris
from plastron.cli.commands import BaseCommand
from plastron.utils import parse_predicate_list
from plastron.rdfmapping.resources import RDFResource

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='reindex',
        description='Reindex objects in the repository'
    )
    parser.add_argument(
        '-R', '--recursive',
        help='Reindex additional objects found by traversing the given predicate(s)',
        action='store'
    )
    parser.add_argument(
        'uris', nargs='*',
        help='URIs of repository objects to reindex'
    )
    parser.set_defaults(cmd_name='reindex')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        self.context.broker.connect()
        self.reindexing_queue = self.context.broker.destination('reindexing'),
        self.username = args.delegated_user or 'plastron'
        traverse = parse_predicate_list(args.recursive) if args.recursive is not None else []
        uris = get_uris(args)

        for uri in uris:
            for resource in self.context.repo[uri].walk(traverse=traverse):
                logger.info(f'Reindexing {resource.url}')
                types = ','.join(resource.describe(RDFResource).rdf_type.values)
                self.context.broker.send(
                    destination=self.reindexing_queue,
                    headers={
                        'CamelFcrepoUri': resource.url,
                        'CamelFcrepoPath': resource.path,
                        'CamelFcrepoResourceType': types,
                        'CamelFcrepoUser': self.username,
                        'persistent': 'true'
                    }
                )

        self.context.broker.disconnect()
