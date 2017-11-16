#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import argparse
import pprint
import rdflib
import sys
import yaml
import logging
import logging.config
from datetime import datetime
from classes import pcdm
from classes.exceptions import RESTAPIException

logger = logging.getLogger(__name__)

#============================================================================
# HELPER FUNCTIONS
#============================================================================

def create_collection(fcrepo, name):
    # open transaction
    logger.info('Opening transaction')
    fcrepo.open_transaction()

    try:
        collection = pcdm.Collection()
        collection.title = name
        collection.create_object(fcrepo)
        collection.update_object(fcrepo)
        # commit transaction
        logger.info('Committing transaction')
        fcrepo.commit_transaction()
        return collection

    except (RESTAPIException) as e:
        # failures here will be caught by the main loop's exception handler
        # and should trigger a system exit
        logger.error("Error in collection creation: {0}".format(e))

#============================================================================
# MAIN Execution
#============================================================================

def main():
    '''Parse args and handle options.'''

    parser = argparse.ArgumentParser(
        description='Collection creation tool for Fedora 4.'
        )

    # Path to the repo config (endpoint, relpath, credentials, and WebAC paths)
    parser.add_argument('-r', '--repo',
                        help='Path to repository configuration file.',
                        action='store',
                        required=True
                        )

    # Name of the collection
    parser.add_argument('-n', '--name',
                        help='Name of the collection.',
                        action='store',
                        required=True
                        )

    # Path to a batch config file
    # If given, will write the collection URI to it
    parser.add_argument('-b', '--batch',
                        help='Path to batch configuration file.',
                        action='store'
                        )

    args = parser.parse_args()


    # configure logging
    with open('config/logging.yml', 'r') as configfile:
        logging_config = yaml.safe_load(configfile)
        logfile = 'logs/create_collection.py.{0}.log'.format(
            datetime.utcnow().strftime('%Y%m%d%H%M%S')
            )
        logging_config['handlers']['file']['filename'] = logfile
        logging.config.dictConfig(logging_config)

    # Load required repository config file and create repository object
    with open(args.repo, 'r') as repoconfig:
        fcrepo = pcdm.Repository(yaml.safe_load(repoconfig))
        logger.info('Loaded repo configuration from {0}'.format(args.repo))

    collection = create_collection(fcrepo, args.name)

    if args.batch is not None:
        with open(args.batch, 'r') as batchconfig:
            batch = yaml.safe_load(batchconfig)
            batch['COLLECTION'] = str(collection.uri)
        with open(args.batch, 'w') as batchconfig:
            yaml.dump(batch, batchconfig, default_flow_style=False)

if __name__ == "__main__":
    main()
