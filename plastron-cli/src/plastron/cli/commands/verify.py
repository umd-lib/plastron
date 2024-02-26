import csv
import logging
from argparse import Namespace

from plastron.cli.commands import BaseCommand


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
    def __call__(self, args: Namespace):
        invalid_items = []
        try:
            with open(args.log) as csvfile:
                reader = csv.DictReader(csvfile)

                for item in reader:
                    query = self.context.solr.search(f'id:\"{item["uri"]}\"')

                    if len(query) == 0:
                        invalid_items.append(item["uri"])
        except OSError as e:
            raise RuntimeError(f'Unable to read {args.log}: {e}')

        if len(invalid_items) > 0:
            logging.info(f"There are {len(invalid_items)} items in the mapfile whose URIs aren't indexed:")
            for item in invalid_items:
                print(item)
        else:
            logging.info("All URIs in the mapfile are indexed!")
