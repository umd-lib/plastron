import logging
import sys
from datetime import datetime
from plastron import util
from plastron.commands import BaseCommand
from plastron.exceptions import RESTAPIException, DataReadException, FailureException
from plastron.models.newspaper import Page
from plastron.http import Transaction

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
    def __call__(self, fcrepo, args):
        fieldnames = ['uri', 'timestamp']

        # read the log of completed items
        try:
            completed = util.ItemLog('logs/annotated.csv', fieldnames, 'uri')
        except Exception as e:
            logger.error('Non-standard map file specified: {0}'.format(e))
            raise FailureException()

        logger.info('Found {0} completed items'.format(len(completed)))

        if args.ignore is not None:
            try:
                ignored = util.ItemLog(args.ignore, fieldnames, 'uri')
            except Exception as e:
                logger.error('Non-standard ignore file specified: {0}'.format(e))
                raise FailureException()
        else:
            ignored = []

        skipfile = 'logs/skipped.extractocr.{0}.csv'.format(now)
        skipped = util.ItemLog(skipfile, fieldnames, 'uri')

        with fcrepo.at_path('/annotations'):
            for line in sys.stdin:
                uri = line.rstrip('\n')
                if uri in completed:
                    continue
                elif uri in ignored:
                    logger.debug('Ignoring {0}'.format(uri))
                    continue

                try:
                    is_extracted = extract(fcrepo, uri)
                except RESTAPIException:
                    logger.error(
                        "Unable to commit or rollback transaction, aborting"
                    )
                    raise FailureException()

                row = {
                    'uri': uri,
                    'timestamp': str(datetime.utcnow())
                }

                if is_extracted:
                    completed.writerow(row)
                else:
                    skipped.writerow(row)


def extract(fcrepo, uri):
    with Transaction(fcrepo) as txn:
        try:
            logger.info("Getting {0} from repository".format(uri))
            page = Page.from_repository(fcrepo, uri)
            logger.info("Creating annotations for page {0}".format(page.title))
            for annotation in page.textblocks():
                annotation.create(fcrepo)
                annotation.update(fcrepo)

            txn.commit()
            return True

        except (RESTAPIException, DataReadException) as e:
            # if anything fails during item creation or committing the transaction
            # attempt to rollback the current transaction
            # failures here will be caught by the main loop's exception handler
            # and should trigger a system exit
            logger.error("OCR extraction failed: {0}".format(e))
            txn.rollback()
            logger.warning('Transaction rolled back. Continuing load.')
