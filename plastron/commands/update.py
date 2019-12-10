import logging

from plastron.exceptions import RESTAPIException
from plastron.util import get_title_string, process_resources, print_header, print_footer

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
        if not args.quiet:
            print_header()

        self.repository = fcrepo
        self.repository.test_connection()
        self.dry_run = args.dry_run

        with open(args.update_file, 'r') as update_file:
            self.sparql_update = update_file.read().encode('utf-8')
        logger.debug(f'SPARQL Update query:\n====BEGIN====\n{self.sparql_update}\n=====END=====')

        if self.dry_run:
            logger.info('Dry run enabled, no actual updates will take place')

        process_resources(
            method=self.update_item,
            repository=self.repository,
            uri_list=args.uris,
            file=args.file,
            recursive=args.recursive,
            use_transaction=args.use_transactions
        )

        if not args.quiet:
            print_footer()

    def update_item(self, resource, graph):
        headers = {'Content-Type': 'application/sparql-update'}
        title = get_title_string(graph)
        if self.dry_run:
            logger.info(f'Would update resource {resource} {title}')
        else:
            response = self.repository.patch(resource.description_uri, data=self.sparql_update, headers=headers)
            if response.status_code == 204:
                logger.info(f'Updated resource {resource} {title}')
            else:
                raise RESTAPIException(response)

