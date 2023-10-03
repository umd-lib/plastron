import importlib
import io
import json
import logging
from argparse import FileType, Namespace
from collections import defaultdict
from email.utils import parsedate_to_datetime

from pyparsing import ParseException

from plastron.client import Client, ClientError
from plastron.cli import get_uris, context
from plastron.cli.commands import BaseCommand
from plastron.utils import strtobool, ItemLog
from plastron.validation import validate
from plastron.repo import RepositoryError, RepositoryResource
from plastron.rdf import parse_predicate_list, get_title_string

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
    def __init__(self, config=None):
        super().__init__(config=config)

    def __call__(self, client: Client, args):
        client.test_connection()
        self.stats = {
            'updated': [],
            'invalid': defaultdict(list),
            'errors': defaultdict(list)
        }

        if args.validate and args.model is None:
            raise RuntimeError("Model must be provided when performing validation")

        if args.model is not None:
            # Retrieve the model to use for validation
            logger.debug(f'Loading model class "{args.model}"')
            try:
                self.model_class = getattr(importlib.import_module("plastron.models"), args.model)
            except AttributeError as e:
                raise RuntimeError(f'Unable to load model "{args.model}"') from e

        self.sparql_update = args.update_file.read().encode('utf-8')

        logger.debug(
            f'SPARQL Update query:\n'
            f'====BEGIN====\n'
            f'{self.sparql_update.decode()}\n'
            f'=====END====='
        )

        if args.dry_run:
            logger.info('Dry run enabled, no actual updates will take place')

        if args.completed:
            completed_log = ItemLog(args.completed, ['uri', 'title', 'timestamp'], 'uri')
        else:
            completed_log = None

        traverse = parse_predicate_list(args.recursive) if args.recursive is not None else []
        uris = get_uris(args)

        for uri in uris:
            with context(repo=self.repo, use_transactions=args.use_transactions, dry_run=args.dry_run):
                for resource in self.repo[uri].walk(traverse=traverse):

                    if completed_log is not None and uri in completed_log:
                        logger.info(f'Resource {uri} has already been updated; skipping')
                        return

                    headers = {'Content-Type': 'application/sparql-update'}
                    title = get_title_string(resource.graph)

                    if args.validate:
                        try:
                            # Apply the update in-memory to the resource graph
                            resource.graph.update(self.sparql_update.decode())
                        except ParseException as parse_error:
                            self.stats['errors'][uri].append(str(parse_error))
                            return

                        # Validate the updated in-memory Graph using the model
                        item = self.model_class.from_graph(resource.graph, subject=uri)
                        validation_result = validate(item)

                        if not validation_result.is_valid():
                            logger.warning(f'Resource {uri} failed validation')
                            self.stats['invalid'][uri].extend(str(failed) for failed in validation_result.failed())
                            return

                    if args.dry_run:
                        logger.info(f'Would update resource {resource} {title}')
                        return

                    request_url = resource.description_url or resource.url

                    response = client.patch(request_url, data=self.sparql_update, headers=headers)
                    if response.status_code == 204:
                        logger.info(f'Updated resource {resource} {title}')
                        timestamp = parsedate_to_datetime(response.headers['date']).isoformat('T')
                        if completed_log is not None:
                                completed_log.append({
                                    'uri': resource.url,
                                    'title': str(title),
                                    'timestamp': timestamp,
                                })
                        self.stats['updated'].append(uri)
                    else:
                        self.stats['errors'][uri].append(str(response))

        if len(self.stats['errors']) == 0 and len(self.stats['invalid']) == 0:
            state = 'update_complete'
        else:
            state = 'update_incomplete'

        self.result = {
            'type': state,
            'stats': self.stats
        }
        logger.debug(self.stats)

    @staticmethod
    def parse_message(message):
        message.body = message.body.encode('utf-8').decode('utf-8-sig')
        body = json.loads(message.body)
        uris = body['uris']
        sparql_update = body['sparql_update']
        dry_run = bool(strtobool(message.args.get('dry-run', 'false')))
        do_validate = bool(strtobool(message.args.get('validate', 'false')))
        # Default to no transactions, due to LIBFCREPO-842
        use_transactions = not bool(strtobool(message.args.get('no-transactions', 'true')))

        return Namespace(
            dry_run=dry_run,
            validate=do_validate,
            model=message.args.get('model', None),
            recursive=message.args.get('recursive', None),
            use_transactions=use_transactions,
            uris=uris,
            update_file=io.StringIO(sparql_update),
            file=None,
            completed=None
        )
