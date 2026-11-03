import logging
from argparse import Namespace
from collections.abc import Mapping
from os import getpid, uname

from plastron.cli import get_uris, version
from plastron.cli.commands import BaseCommand
from plastron.messaging.broker import Destination
from plastron.messaging.messages import Message
from plastron.rdfmapping.resources import RDFResource
from plastron.repo import Tombstone, RepositoryResource
from plastron.utils import parse_predicate_list

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(name='reindex', description='Reindex objects in the repository')
    parser.add_argument(
        '-R',
        '--recursive',
        help='reindex additional objects found by traversing the given predicate(s)',
        action='store',
        metavar='PREDICATES',
    )
    parser.add_argument(
        '-i',
        '--index',
        help='configuration key for the index to target; defaults to "all"',
        action='store',
        metavar='KEY',
        default='all',
    )
    parser.add_argument(
        'uris',
        nargs='*',
        help='URI of repository object to reindex',
        metavar='uri',
    )
    parser.set_defaults(cmd_name='reindex')


class Command(BaseCommand):
    def get_routing_headers(self, index_key: str):
        all_routing_headers = self.config['ROUTING_HEADERS']
        try:
            return all_routing_headers[index_key]
        except KeyError as e:
            raise RuntimeError(
                f'"{e}" is not a recognized index routing name. Use one of: {", ".join(all_routing_headers.keys())}'
            ) from e

    def create_message(
        self,
        resource: RepositoryResource | Tombstone,
        username: str,
        routing_headers: Mapping[str, str],
    ) -> tuple[Destination, Message]:
        if isinstance(resource, Tombstone):
            # If the requested resource has been deleted (i.e., it returns a tombstone), then "reindexing"
            # is implemented by re-sending the delete event to the indexing queue, so the routing will remove
            # the resource from the indexes
            logger.info(f'Resource {resource.url} has been removed, sending message to delete from indexes')
            return (
                self.context.broker.destination('indexing'),
                Message(
                    headers={
                        'FcrepoEventName': 'delete',
                        'FcrepoUri': resource.url,
                        'FcrepoPath': resource.path,
                        'FcrepoUser': username,
                        **routing_headers,
                    },
                    persistent='true',
                ),
            )
        else:
            types = ','.join(resource.describe(RDFResource).rdf_type.values)
            return (
                self.context.broker.destination('reindexing'),
                Message(
                    headers={
                        'FcrepoUri': resource.url,
                        'FcrepoPath': resource.path,
                        'FcrepoResourceType': types,
                        'FcrepoUser': username,
                        **routing_headers,
                    },
                    persistent='true',
                ),
            )

    def __call__(self, args: Namespace):
        routing_headers = self.get_routing_headers(args.index)
        logger.info(f'Indexing to {args.index}')
        if self.context.broker.connect(client_id=f'plastrond/{version}-{uname().nodename}-{getpid()}'):
            username = args.delegated_user or 'plastron'
            traverse = parse_predicate_list(args.recursive) if args.recursive is not None else []
            uris = get_uris(args)

            for uri in uris:
                for resource in self.context.repo[uri].walk(traverse=traverse, include_tombstones=True):
                    logger.info(f'Reindexing {resource.url}')
                    destination, message = self.create_message(resource, username, routing_headers)
                    destination.send(message=message)

            self.context.broker.disconnect()
        else:
            raise RuntimeError(f'STOMP connection failed for {self.context.broker}')
