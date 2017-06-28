#!/usr/bin/env python3

import argparse
import sys
import yaml
import logging
import logging.config
from datetime import datetime
from classes import pcdm,ocr,util
from handler import ndnp
import rdflib
from rdflib import RDF
from lxml import etree as ET

logger = logging.getLogger(__name__)

def extract(fcrepo, uri):
    fcrepo.open_transaction()
    try:
        logger.info("Getting {0} from repository".format(uri))
        page = ndnp.Page.from_repository(fcrepo, uri)
        logger.info("Creating annotations for page {0}".format(page.title))
        for annotation in page.textblocks():
            annotation.create_object(fcrepo)
            annotation.update_object(fcrepo)

        fcrepo.commit_transaction()
        return True

    except (pcdm.RESTAPIException, ndnp.DataReadException) as e:
        # if anything fails during item creation or commiting the transaction
        # attempt to rollback the current transaction
        # failures here will be caught by the main loop's exception handler
        # and should trigger a system exit
        logger.error("OCR extraction failed: {0}".format(e))
        fcrepo.rollback_transaction()
        logger.warn('Transaction rolled back. Continuing load.')

def main():
    '''Parse args and handle options.'''

    parser = argparse.ArgumentParser(
        description='Extract OCR text and create annotations.'
        )

    # Path to the repo config (endpoint, relpath, credentials, and WebAC paths)
    parser.add_argument('-r', '--repo',
                        help='Path to repository configuration file.',
                        action='store',
                        required=True
                        )

    parser.add_argument('uris', nargs='+',
                        help='One or more repository URIs to extract text from.'
                        )

    args = parser.parse_args()

    now = datetime.utcnow().strftime('%Y%m%d%H%M%S')

    # configure logging
    with open('config/logging.yml', 'r') as configfile:
        logging_config = yaml.safe_load(configfile)
        logfile = 'logs/extractocr.py.{0}.log'.format(now)
        logging_config['handlers']['file']['filename'] = logfile
        logging.config.dictConfig(logging_config)

    # Load required repository config file and create repository object
    with open(args.repo, 'r') as repoconfig:
        fcrepo = pcdm.Repository(yaml.safe_load(repoconfig))
        logger.info('Loaded repo configuration from {0}'.format(args.repo))

    # read the log of completed items
    try:
        completed = util.ItemLog('logs/annotated.csv', ['uri', 'timestamp'], 'uri')
    except Exception as e:
        logger.error('Non-standard map file specified: {0}'.format(e))
        sys.exit(1)

    logger.info('Found {0} completed items'.format(len(completed)))

    skipfile = 'logs/skipped.extractocr.{0}.csv'.format(now)
    skipped = util.ItemLog(skipfile, ['uri', 'timestamp'], 'uri')

    with fcrepo.at_path('/annotations'):
        for uri in args.uris:
            if uri in completed:
                continue

            is_extracted = False
            try:
                is_extracted = extract(fcrepo, uri)
            except pcdm.RESTAPIException as e:
                logger.error(
                    "Unable to commit or rollback transaction, aborting"
                    )
                sys.exit(1)

            row = {
                'uri': uri,
                'timestamp': str(datetime.utcnow())
                }

            if is_extracted:
                completed.writerow(row)
            else:
                skipped.writerow(row)

if __name__ == "__main__":
    main()
