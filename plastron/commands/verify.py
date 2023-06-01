import csv
import logging
import pysolr
import os
import yaml

from argparse import Namespace
from dataclasses import dataclass, field
from plastron.exceptions import FailureException
from plastron.jobs import ItemLog
from plastron.commands import BaseCommand


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='verify',
        description="Verify item URI's are indexed in Solr and display URI's that aren't"
    )
    parser.add_argument(
        '-l', '--log',
        help='completed log file from an import job',
        action='store',
        default=None
    )

    parser.set_defaults(cmd_name='verify')


class Command(BaseCommand):
    def __init__(self, config=None):
        super().__init__(config=config)
        self.invalid_items = []

    def __call__(self, fcrepo, args: Namespace):
        if not os.path.isfile(args.log):
            raise FailureException('Path to log file is not valid')

        with open(args.log) as csvfile:
            reader = csv.DictReader(csvfile)

            for item in reader:
                query = self.solr.search(f'id:\"{item["uri"]}\"')

                if len(query) == 0:
                    self.invalid_items.append(item["uri"])
        
        if len(self.invalid_items) > 0:
            logging.info("There are items in the mapfile whose URI's aren't indexed")
            for item in self.invalid_items:
                print(item)
