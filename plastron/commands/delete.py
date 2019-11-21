import logging
from plastron.util import get_title_string, print_header, print_footer, process_resources

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='delete',
        aliases=['del', 'rm'],
        description='Delete objects from the repository')
    parser.add_argument(
        '-R', '--recursive',
        help='Delete additional objects found by traversing the given predicate(s)',
        action='store'
    )
    parser.add_argument(
        '-d', '--dryrun',
        help='Simulate a delete without modifying the repository',
        action='store_true'
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
        if not args.quiet:
            print_header()

        self.repository = fcrepo
        self.repository.test_connection()
        self.dry_run = args.dryrun

        if self.dry_run:
            logger.info('Dry run enabled, no actual deletions will take place')

        process_resources(
            method=self.delete_item,
            repository=self.repository,
            uri_list=args.uris,
            file=args.file,
            recursive=args.recursive,
            use_transaction=(not args.dryrun)
        )

        if not args.quiet:
            print_footer()

    def delete_item(self, target_uri, graph):
        title = get_title_string(graph)
        if self.dry_run:
            logger.info(f'Would delete resource {target_uri} {title}')
        else:
            self.repository.delete(target_uri)
            logger.info(f'Deleted resource {target_uri} {title}')
