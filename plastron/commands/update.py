import importlib
import io
import json
import logging
from argparse import Namespace
from distutils.util import strtobool
from email.utils import parsedate_to_datetime

from plastron.commands import BaseCommand
from plastron.exceptions import FailureException, RESTAPIException
from plastron.ldp import Resource
from plastron.util import get_title_string, ResourceList, parse_predicate_list
from pyparsing import ParseException

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


def validate(item):
    result = item.validate()

    if result.is_valid():
        logger.info(f'"{item}" is valid')
        for outcome in result.passed():
            logger.debug(f'  ✓ {outcome}')
    else:
        logger.warning(f'{item} is invalid')
        for outcome in result.failed():
            logger.warning(f'  ✗ {outcome}')
        for outcome in result.passed():
            logger.debug(f'  ✓ {outcome}')

    return result


class Command(BaseCommand):
    def __init__(self, config=None):
        super().__init__(config=config)
        self.result = None
        self.repository = None
        self.dry_run = False
        self.sparql_update = None
        self.resources = None
        self.validate = False
        self.model = None

    def __call__(self, fcrepo, args):
        self.execute(fcrepo, args)

    def execute(self, fcrepo, args):
        self.repository = fcrepo
        self.repository.test_connection()
        self.dry_run = args.dry_run
        self.validate = args.validate
        self.model = args.model

        if self.validate and not self.model:
            raise FailureException("Model must be provided when performing validation")

        # args.update_file is a StringIO when coming from the daemon
        # (see "parse_message" method), a regular file when coming from the CLI
        if isinstance(args.update_file, io.StringIO):
            self.sparql_update = args.update_file.getvalue().encode('utf-8')
        else:
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
        if self.resources.completed and resource.uri in self.resources.completed:
            logger.info(f'Resource {resource.uri} has already been updated; skipping')
            return
        headers = {'Content-Type': 'application/sparql-update'}
        title = get_title_string(graph)

        errors = []

        if self.validate:
            try:
                # Apply the update in-memory to the resource graph
                graph.update(self.sparql_update.decode())
            except ParseException as parse_error:
                errors.append(parse_error)
                raise FailureException(errors)

            # Retrieve the model to use for validation
            try:
                model_class = getattr(importlib.import_module("plastron.models"), self.model)
            except AttributeError as e:
                raise FailureException(f'Unable to load model "{self.model}"') from e

            # Validate the updated in-memory Graph using the model
            issue = model_class.from_graph(graph, subject=resource.uri)
            validation_result = validate(issue)

            is_valid = validation_result.is_valid()
            if not is_valid:
                for failed in validation_result.failed():
                    errors.append(str(failed))
                raise FailureException(errors)

        if self.dry_run:
            logger.info(f'Would update resource {resource} {title}')
            return

        response = self.repository.patch(resource.description_uri, data=self.sparql_update, headers=headers)
        if response.status_code == 204:
            logger.info(f'Updated resource {resource} {title}')
            timestamp = parsedate_to_datetime(response.headers['date']).isoformat('T')
            self.resources.log_completed(resource.uri, title, timestamp)
        else:
            raise RESTAPIException(response)

    @staticmethod
    def parse_message(message):
        message.body = message.body.encode('utf-8').decode('utf-8-sig')
        body = json.loads(message.body)
        uris = body['uri']
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
