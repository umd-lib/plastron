import logging

from rdflib import URIRef

from plastron.commands import BaseCommand
from plastron.core.util import ResourceList
from plastron.namespaces import rdf
from plastron.rdf import parse_predicate_list

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
    def __init__(self, config=None):
        super().__init__(config=config)
        self.broker = None
        self.repo = None

    def __call__(self, repo, args):
        self.broker.connect()
        self.reindexing_queue = self.broker.destination('reindexing'),
        self.username = args.delegated_user or 'plastron'
        resources = ResourceList(
            client=self.repo,
            uri_list=args.uris
        )

        resources.process(
            method=self.reindex_item,
            traverse=parse_predicate_list(args.recursive),
            use_transaction=False
        )
        self.broker.disconnect()

    def reindex_item(self, resource, graph):
        logger.info(f'Reindexing {resource.uri}')
        types = ','.join(graph.objects(subject=URIRef(resource.uri), predicate=rdf.type))
        self.broker.send(
            destination=self.reindexing_queue,
            headers={
                'CamelFcrepoUri': resource.uri,
                'CamelFcrepoPath': self.repo.repo_path(resource.uri),
                'CamelFcrepoResourceType': types,
                'CamelFcrepoUser': self.username,
                'persistent': 'true'
            }
        )
