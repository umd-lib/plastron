import logging
from argparse import FileType, Namespace

from plastron.cli.commands import BaseCommand
from plastron.jobs.updatejob import UpdateJob
from plastron.models import get_model_from_name
from plastron.utils import parse_predicate_list
from plastron.jobs import ItemLog

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='update',
        description='Update objects in the repository'
    )
    parser.add_argument(
        '-u', '--update-file',
        help='Path to SPARQL Update file to apply',
        type=FileType(mode='r'),
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
        '--validate',
        help='validate before updating',
        action='store_true',
        dest='validate'
    )
    parser.add_argument(
        '-m', '--model',
        help='The model class to use for validation (Item, Issue, Poster, or Letter)',
        action='store',
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


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        self.context.client.test_connection()

        if args.validate and args.model is None:
            raise RuntimeError("Model must be provided when performing validation")

        # Retrieve the model to use for validation
        model_class = get_model_from_name(args.model) if args.model else None

        sparql_update = args.update_file.read().encode('utf-8')

        if args.dry_run:
            logger.info('Dry run enabled, no actual updates will take place')

        if args.completed and not args.dry_run:
            completed_log = ItemLog(args.completed, ['uri', 'title', 'timestamp'], 'uri')
        else:
            completed_log = None

        traverse = parse_predicate_list(args.recursive) if args.recursive is not None else []

        update_job = UpdateJob(
            repo=self.context.repo,
            uris=args.uris,
            sparql_update=sparql_update,
            model_class=model_class,
            traverse=traverse,
            completed=completed_log,
            dry_run=args.dry_run,
        )
        self.run(update_job.run())
