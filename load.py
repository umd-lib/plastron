#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import csv
import logging
from importlib import import_module
import os.path
import pprint
import rdflib
import requests
import sys
import yaml
import re
import logging
import logging.config
from datetime import datetime
from classes import pcdm

logger = logging.getLogger(__name__)

#============================================================================
# HELPER FUNCTIONS
#============================================================================

def print_header():
    '''Common header formatting.'''
    title = '|     FCREPO BATCH LOADER     |'
    bar = '+' + '='*(len(title)-2) + '+'
    spacer = '|' + ' '*(len(title)-2) + '|'
    print('\n'.join(['', bar, spacer, title, spacer, bar, '']))

def print_footer():
    '''Report success or failure and resources created.'''
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

def load_item(fcrepo, item, args, extra=None):
    # open transaction
    logger.info('Opening transaction')
    fcrepo.open_transaction()

    # create item and its components
    try:
        logger.info('Creating item')
        item.recursive_create(fcrepo, args.nobinaries)
        logger.info('Creating ordering proxies')
        item.create_ordering(fcrepo)
        logger.info('Updating relationship triples')
        item.update_relationship_triples()

        if extra:
            logger.info('Adding additional triples')
            if re.search(r'\.(ttl|n3|nt)$', extra):
                rdf_format = 'n3'
            elif re.search(r'\.(rdf|xml)$', extra):
                rdf_format = 'xml'
            item.add_extra_properties(extra, rdf_format)

        logger.info('Updating item and components')
        item.recursive_update(fcrepo, args.nobinaries)

        # commit transaction
        logger.info('Committing transaction')
        fcrepo.commit_transaction()
        return True

    except (pcdm.RESTAPIException, FileNotFoundError) as e:
        # if anything fails during item creation or commiting the transaction
        # attempt to rollback the current transaction
        # failures here will be caught by the main loop's exception handler
        # and should trigger a system exit
        logger.error("Item creation failed: {0}".format(e))
        fcrepo.rollback_transaction()
        logger.warn('Transaction rolled back. Continuing load.')

#============================================================================
# MAIN LOOP
#============================================================================

def main():
    '''Parse args and handle options.'''

    parser = argparse.ArgumentParser(
        description='A configurable batch loader for Fedora 4.'
        )

    # Path to the repo config (endpoint, relpath, credentials, and WebAC paths)
    parser.add_argument('-r', '--repo',
                        help='Path to repository configuration file.',
                        action='store',
                        required=True
                        )

    # Data handler module to use
    parser.add_argument('-b', '--batch',
                        help='Path to batch configuration file.',
                        action='store',
                        required=True
                        )

    # Run through object preparation, but do not touch repository
    parser.add_argument('-d', '--dryrun',
                        help='Iterate over the batch without POSTing.',
                        action='store_true'
                        )

    # Load without binaries; useful for testing when file loading is too slow
    parser.add_argument('-n', '--nobinaries',
                        help='Iterate without uploading binaries.',
                        action='store_true'
                        )

    # Limit the load to a specified number of top-level objects
    parser.add_argument('-l', '--limit',
                        help='''Limit the load to a specified number of
                                top-level objects.''',
                        action='store',
                        type=int,
                        default=None
                        )

    # Just ping the repository to see if the endpoint exists
    parser.add_argument('-p', '--ping',
                        help='Check the connection to the repository and exit.',
                        action='store_true'
                        )

    parser.add_argument('-v', '--verbose',
                        help='Increase the verbosity of the status output.',
                        action='store_true'
                        )

    parser.add_argument('-q', '--quiet',
                        help='Decrease the verbosity of the status output.',
                        action='store_true'
                        )

    args = parser.parse_args()

    if not args.quiet:
        print_header()

    # configure logging
    with open('config/logging.yml', 'r') as configfile:
        logging_config = yaml.safe_load(configfile)
        if args.verbose:
            logging_config['handlers']['console']['level'] = 'DEBUG'
        elif args.quiet:
            logging_config['handlers']['console']['level'] = 'WARNING'
        logfile = 'logs/load.py.{0}.log'.format(
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

    # Load required batch config file and create batch object
    with open(args.batch, 'r') as batchconfig:
        batch_options = yaml.safe_load(batchconfig)
        logger.info(
            'Loaded batch configuration from {0}'.format(args.batch)
            )
        # Define the data_handler function for the data being loaded
        logger.info("Initializing data handler")
        module_name = batch_options.get('HANDLER')
        handler = import_module('handler.' + module_name)
        logger.info('Loaded "{0}" handler'.format(module_name))

        # Handler is invoked by calling the load function on the batch config
        try:
            batch = handler.load(fcrepo, batch_options)
        except handler.ConfigException as e:
            logger.error(e.message)
            logger.error("Failed to load batch configuration from {0}".format(args.batch))
            sys.exit(1)

    if not args.dryrun:
        test_connection(fcrepo)

        # open mapfile, if it exists, and read completed files into list
        fieldnames = ['number', 'timestamp', 'title', 'path', 'uri']
        completed_items = []
        skip_list = []
        mapfile = batch_options.get('MAPFILE')
        if os.path.isfile(mapfile):
            with open(mapfile, 'r') as infile:
                reader = csv.DictReader(infile)
                # check the validity of the map file data
                if not reader.fieldnames == fieldnames:
                    logger.error('Non-standard map file specified!')
                    sys.exit(1)
                else:
                    # read the data from the existing file
                    completed_items = [row for row in reader]
                    skip_list = [row['path'] for row in completed_items]

        # open a new version of the map file
        with open(mapfile, 'w+') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            # write out completed items
            logger.info(
                'Writing data for {0} existing items to mapfile.'.format(
                    len(completed_items))
                    )
            for row in completed_items:
                writer.writerow(row)

            # create all batch objects in repository
            for n, item in enumerate(batch):
                if args.limit is not None and n >= args.limit:
                    logger.info("Stopping after {0} item(s)".format(args.limit))
                    break
                elif item.path in skip_list:
                    continue

                logger.info(
                    "Processing item {0}/{1}...".format(n+1, batch.length)
                    )
                if args.verbose:
                    item.print_item_tree()

                try:
                    logger.info('Loading item {0}'.format(n+1))
                    is_loaded = load_item(
                        fcrepo, item, args, extra=batch_options.get('EXTRA')
                        )
                except pcdm.RESTAPIException as e:
                    logger.error(
                        "Unable to commit or rollback transaction, aborting"
                        )
                    sys.exit(1)

                if is_loaded:
                    # write item details to mapfile
                    row = {'number': n + 1,
                           'timestamp': item.creation_timestamp,
                           'title': item.title,
                           'path': item.path,
                           'uri': item.uri
                           }
                    writer.writerow(row)

    if not args.quiet:
        print_footer()

if __name__ == "__main__":
    main()
