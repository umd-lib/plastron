import logging
from email.utils import parsedate_to_datetime

from plastron.commands import BaseCommand
from plastron.exceptions import RESTAPIException
from plastron.util import get_title_string, ResourceList, parse_predicate_list

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='delete',
        aliases=['del', 'rm'],
        description='Delete objects from the repository'
    )
    parser.add_argument(
        '-R', '--recursive',
        help='Delete additional objects found by traversing the given predicate(s)',
        action='store'
    )
    parser.add_argument(
        '-d', '--dry-run',
        help='Simulate a delete without modifying the repository',
        action='store_true'
    )
    parser.add_argument(
        '--no-transactions', '--no-txn',
        help='run the update without using transactions',
        action='store_false',
        dest='use_transactions'
    )
    parser.add_argument(
        '--completed',
        help='file recording the URIs of deleted resources',
        action='store'
    )
    parser.add_argument(
        '-f', '--file',
        help='File containing a list of URIs to delete',
        action='store'
    )
    parser.add_argument(
        'uris', nargs='*',
        help='Repository URIs to be deleted.'
    )
    parser.set_defaults(cmd_name='delete')


class Command(BaseCommand):
    def __call__(self, fcrepo, args):
        self.repository = fcrepo
        self.repository.test_connection()
        self.dry_run = args.dry_run

        if self.dry_run:
            logger.info('Dry run enabled, no actual deletions will take place')

        self.resources = ResourceList(
            repository=self.repository,
            uri_list=args.uris,
            file=args.file,
            completed_file=args.completed
        )
        self.resources.process(
            method=self.delete_item,
            traverse=parse_predicate_list(args.recursive),
            use_transaction=args.use_transactions
        )

    def delete_item(self, resource, graph):
        if self.resources.completed and resource.uri in self.resources.completed:
            logger.info(f'Resource {resource.uri} has already been deleted; skipping')
            return
        title = get_title_string(graph)
        if self.dry_run:
            logger.info(f'Would delete resource {resource} {title}')
        else:
            response = self.repository.delete(resource.uri)
            if response.status_code == 204:
                logger.info(f'Deleted resource {resource} {title}')
                timestamp = parsedate_to_datetime(response.headers['date']).isoformat('T')
                self.resources.log_completed(resource.uri, title, timestamp)
            else:
                raise RESTAPIException(response)
