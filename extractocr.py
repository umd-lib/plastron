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

    # configure logging
    with open('config/logging.yml', 'r') as configfile:
        logging_config = yaml.safe_load(configfile)
        logfile = 'logs/extractocr.py.{0}.log'.format(
            datetime.utcnow().strftime('%Y%m%d%H%M%S')
            )
        logging_config['handlers']['file']['filename'] = logfile
        logging.config.dictConfig(logging_config)

    # Load required repository config file and create repository object
    with open(args.repo, 'r') as repoconfig:
        fcrepo = pcdm.Repository(yaml.safe_load(repoconfig))
        logger.info('Loaded repo configuration from {0}'.format(args.repo))

    # read the log of completed items
    try:
        completed = util.CompletedLog('logs/annotated.csv', ['uri', 'timestamp'], 'uri')
    except Exception as e:
        logger.error('Non-standard map file specified: {0}'.format(e))
        sys.exit(1)

    logger.info('Found {0} completed items'.format(len(completed)))

    with fcrepo.at_path('/annotations'):
        for uri in args.uris:
            if uri in completed:
                continue

            fcrepo.open_transaction()
            page = ndnp.Page.from_repository(fcrepo, uri)
            logger.info("Creating annotations for page {0}".format(page.title))
            for annotation in page.textblocks():
                annotation.create_object(fcrepo)
                annotation.update_object(fcrepo)
            fcrepo.commit_transaction()
            completed.writerow({
                'uri': uri,
                'timestamp': str(datetime.utcnow())
                })

if __name__ == "__main__":
    main()
