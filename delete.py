#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function

import argparse
from datetime import datetime
import logging
import logging.config
from classes import pcdm
import requests
import sys
import yaml

logger = logging.getLogger(__name__)


#============================================================================
# HELPER FUNCTIONS
#============================================================================

def print_header():
    '''Common header formatting.'''
    title = '|     FCREPO BATCH DELETE     |'
    bar = '+' + '='*(len(title)-2) + '+'
    spacer = '|' + ' '*(len(title)-2) + '|'
    print('\n'.join(['', bar, spacer, title, spacer, bar, '']))

def print_footer():
    print('\nScript complete. Goodbye!\n')

def test_connection(fcrepo):
    # test connection to fcrepo
    logger.debug("fcrepo.endpoint = %s", fcrepo.fullpath)
    logger.debug("fcrepo.relpath = %s", fcrepo.fullpath)
    logger.debug("fcrepo.fullpath = %s", fcrepo.fullpath)
    logger.info("Testing connection to {0}".format(fcrepo.fullpath))
    if fcrepo.is_reachable():
        logger.info("Connection successful.")
    else:
        logger.warn("Unable to connect.")
        sys.exit(1)

def delete_item(fcrepo, args):
    # open transaction
    logger.info('Opening transaction')
    fcrepo.open_transaction()

    # create item and its components
    try:
        for uri in args.uris:
            logger.info('Deleting resource {0}'.format(uri))
            fcrepo.delete(uri)
            logger.info('Deleted resource {0}'.format(uri))

        # commit transaction
        logger.info('Committing transaction')
        fcrepo.commit_transaction()
        return True

    except pcdm.RESTAPIException as e:
        # if anything fails during deletion of a set of uris, attempt to
        # rollback the transaction. Failures here will be caught by the main         
        # loop's exception handler and should trigger a system exit
        logger.error("Item creation failed: {0}".format(e))
        fcrepo.rollback_transaction()
        logger.warn('Transaction rolled back.')


#============================================================================
# MAIN LOOP
#============================================================================

def main():
    '''Parse args and handle options.'''

    parser = argparse.ArgumentParser(
        description='Delete tool for Fedora 4.'
        )

    # Path to the repo config (endpoint, relpath, credentials, and WebAC paths)
    parser.add_argument('-r', '--repo',
                        help='Path to repository configuration file.',
                        action='store',
                        required=True
                        )

    # Just ping the repository to verify the connection
    parser.add_argument('-p', '--ping',
                        help='Check the connection to the repository and exit.',
                        action='store_true'
                        )

    parser.add_argument('uris', nargs='+',
                        help='One or more repository URIs to be deleted.'
                        )

    args = parser.parse_args()
    
    print_header()
    
    # configure logging
    with open('config/logging.yml', 'r') as configfile:
        logging_config = yaml.safe_load(configfile)
        logfile = 'logs/delete.py.{0}.log'.format(
            datetime.utcnow().strftime('%Y%m%d%H%M%S')
            )
        logging_config['handlers']['file']['filename'] = logfile
        logging.config.dictConfig(logging_config)

    # Load required repository config file and create repository object
    with open(args.repo, 'r') as repoconfig:
        fcrepo = pcdm.Repository(yaml.safe_load(repoconfig))
        logger.info('Loaded repo configuration from {0}'.format(args.repo))

    # "--ping" tests repository connection and exits
    if args.ping:
        test_connection(fcrepo)
        sys.exit(0)
    
    test_connection(fcrepo)
    try:
        delete_item(fcrepo, args)
    except pcdm.RESTAPIException as e:
        logger.error(
            "Unable to commit or rollback transaction, aborting"
            )
        sys.exit(1)
    
    print_footer()

if __name__ == "__main__":
    main()
