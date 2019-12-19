import logging
from email.utils import parsedate_to_datetime

from plastron.exceptions import FailureException, RESTAPIException
from plastron.util import get_title_string, ResourceList, parse_predicate_list, ItemLog

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


class Command:
    def __call__(self, fcrepo, args):
        self.repository = fcrepo
        self.repository.test_connection()
        self.dry_run = args.dry_run

        if self.dry_run:
            logger.info('Dry run enabled, no actual deletions will take place')

        if args.completed:
            logger.info(f'Reading the completed items log from {args.completed}')
            # read the log of completed items
            fieldnames = ['uri', 'title', 'timestamp']
            try:
                self.completed = ItemLog(args.completed, fieldnames, 'uri')
                logger.info(f'Found {len(self.completed)} completed item(s)')
            except Exception as e:
                logger.error(f"Non-standard map file specified: {e}")
                raise FailureException()
        else:
            self.completed = []

        resources = ResourceList(
            repository=self.repository,
            uri_list=args.uris,
            file=args.file
        )
        resources.process(
            method=self.delete_item,
            traverse=parse_predicate_list(args.recursive),
            use_transaction=args.use_transactions
        )

    def delete_item(self, resource, graph):
        if resource.uri in self.completed:
            logger.info(f'Resource {resource.uri} has already been deleted; skipping')
            return
        title = get_title_string(graph)
        if self.dry_run:
            logger.info(f'Would delete resource {resource} {title}')
        else:
            response = self.repository.delete(resource.uri)
            if response.status_code == 204:
                logger.info(f'Deleted resource {resource} {title}')
                self.completed.append({
                    'uri': resource.uri,
                    'title': title,
                    'timestamp': parsedate_to_datetime(response.headers['date']).isoformat('T')
                })
            else:
                raise RESTAPIException(response)
