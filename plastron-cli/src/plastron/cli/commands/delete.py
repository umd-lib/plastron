import logging
from argparse import FileType, Namespace
from datetime import datetime

from plastron.cli import get_uris
from plastron.repo.utils import context
from plastron.cli.commands import BaseCommand
from plastron.client import ClientError
from plastron.models.pcdm import PCDMObject
from plastron.repo import RepositoryError
from plastron.utils import parse_predicate_list
from plastron.jobs import ItemLog

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
        dest='uris_file',
        type=FileType(mode='r'),
        help='File containing a list of URIs to delete',
        action='store'
    )
    parser.add_argument(
        'uris', nargs='*',
        help='Repository URIs to be deleted.'
    )
    parser.set_defaults(cmd_name='delete')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        self.context.client.test_connection()
        if args.completed:
            completed_log = ItemLog(args.completed, ['uri', 'title', 'timestamp'], 'uri')
        else:
            completed_log = None

        if args.dry_run:
            logger.info('Dry run enabled, no actual deletions will take place')

        if args.recursive:
            traverse = parse_predicate_list(args.recursive)
        else:
            # if recursive was not specified, traverse nothing
            traverse = []

        uris = get_uris(args)

        for uri in uris:
            with context(repo=self.context.repo, use_transactions=args.use_transactions, dry_run=args.dry_run):
                try:
                    for resource in self.context.repo[uri].walk(traverse=traverse):
                        obj = resource.describe(PCDMObject)
                        title = obj.title
                        if args.dry_run:
                            logger.info(f'Would delete resource {resource.url} "{title}"')
                            continue
                        resource.delete()
                        if completed_log is not None:
                            completed_log.append({
                                'uri': resource.url,
                                'title': str(title),
                                'timestamp': datetime.now().isoformat('T'),
                            })
                except RepositoryError as e:
                    if isinstance(e.__cause__, ClientError) and e.__cause__.status_code in (404, 410):
                        # not a problem to try and delete something that is not there
                        logger.info(f'Resource {uri} does not exist; skipping')
                    else:
                        raise
