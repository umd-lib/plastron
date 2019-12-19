import logging
from email.utils import parsedate_to_datetime
from plastron.exceptions import RESTAPIException
from plastron.util import get_title_string, ResourceList, parse_predicate_list

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='update',
        description='Update objects in the repository'
    )
    parser.add_argument(
        '-u', '--update-file',
        help='Path to SPARQL Update file to apply',
        action='store',
        required=True
    )
    parser.add_argument(
        '-R', '--recursive',
        help='Update additional objects found by traversing the given predicate(s)',
        action='store'
    )
    parser.add_argument(
        '-d', '--dry-run',
        help='Simulate an update without modifying the repository',
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
        help='file recording the URIs of updated resources',
        action='store'
    )
    parser.add_argument(
        '-f', '--file',
        help='File containing a list of URIs to update',
        action='store'
    )
    parser.add_argument(
        'uris', nargs='*',
        help='URIs of repository objects to update'
    )
    parser.set_defaults(cmd_name='update')


class Command:
    def __call__(self, fcrepo, args):
        self.repository = fcrepo
        self.repository.test_connection()
        self.dry_run = args.dry_run

        with open(args.update_file, 'r') as update_file:
            self.sparql_update = update_file.read().encode('utf-8')
        logger.debug(
            f'SPARQL Update query:\n'
            f'====BEGIN====\n'
            f'{self.sparql_update.decode()}\n'
            f'=====END====='
        )

        if self.dry_run:
            logger.info('Dry run enabled, no actual updates will take place')

        self.resources = ResourceList(
            repository=self.repository,
            uri_list=args.uris,
            file=args.file,
            completed_file=args.completed
        )
        self.resources.process(
            method=self.update_item,
            traverse=parse_predicate_list(args.recursive),
            use_transaction=args.use_transactions
        )

    def update_item(self, resource, graph):
        if resource.uri in self.resources.completed:
            logger.info(f'Resource {resource.uri} has already been updated; skipping')
            return
        headers = {'Content-Type': 'application/sparql-update'}
        title = get_title_string(graph)
        if self.dry_run:
            logger.info(f'Would update resource {resource} {title}')
        else:
            response = self.repository.patch(resource.description_uri, data=self.sparql_update, headers=headers)
            if response.status_code == 204:
                logger.info(f'Updated resource {resource} {title}')
                timestamp = parsedate_to_datetime(response.headers['date']).isoformat('T')
                self.resources.log_completed(resource.uri, title, timestamp)
            else:
                raise RESTAPIException(response)
