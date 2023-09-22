import logging
import sys
from datetime import datetime

from plastron.client import Client, TransactionClient, ClientError
from plastron.cli.commands import BaseCommand
from plastron.repo import DataReadError
from plastron.models.newspaper import Page
from plastron.utils import ItemLog

logger = logging.getLogger(__name__)
now = datetime.utcnow().strftime('%Y%m%d%H%M%S')


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='extractocr',
        description='Create annotations from OCR data stored in the repository'
    )
    parser.add_argument(
        '--ignore', '-i',
        help='file listing items to ignore',
        action='store'
    )
    parser.set_defaults(cmd_name='extractocr')


class Command(BaseCommand):
    def __call__(self, client: Client, args):
        fieldnames = ['uri', 'timestamp']

        # read the log of completed items
        try:
            completed = ItemLog('logs/annotated.csv', fieldnames, 'uri')
        except Exception as e:
            logger.error('Non-standard map file specified: {0}'.format(e))
            raise RuntimeError()

        logger.info('Found {0} completed items'.format(len(completed)))

        if args.ignore is not None:
            try:
                ignored = ItemLog(args.ignore, fieldnames, 'uri')
            except Exception as e:
                logger.error('Non-standard ignore file specified: {0}'.format(e))
                raise RuntimeError()
        else:
            ignored = []

        skipfile = 'logs/skipped.extractocr.{0}.csv'.format(now)
        skipped = ItemLog(skipfile, fieldnames, 'uri')

        for line in sys.stdin:
            uri = line.rstrip('\n')
            if uri in completed:
                continue
            elif uri in ignored:
                logger.debug('Ignoring {0}'.format(uri))
                continue

            try:
                is_extracted = extract(client, uri)
            except ClientError:
                logger.error(
                    "Unable to commit or rollback transaction, aborting"
                )
                raise RuntimeError()

            row = {
                'uri': uri,
                'timestamp': str(datetime.utcnow())
            }

            if is_extracted:
                completed.writerow(row)
            else:
                skipped.writerow(row)


def extract(client: Client, uri):
    with client.transaction() as txn_client:  # type: TransactionClient
        try:
            logger.info("Getting {0} from repository".format(uri))
            page = Page.from_repository(txn_client, uri)
            logger.info("Creating annotations for page {0}".format(page.title))
            for annotation in page.textblocks():
                annotation.create(txn_client)
                annotation.update(txn_client)

            txn_client.commit()
            return True

        except (ClientError, DataReadError) as e:
            # if anything fails during item creation or committing the transaction
            # attempt to roll back the current transaction
            # failures here will be caught by the main loop's exception handler
            # and should trigger a system exit
            logger.error("OCR extraction failed: {0}".format(e))
            txn_client.rollback()
            logger.warning('Transaction rolled back. Continuing load.')
