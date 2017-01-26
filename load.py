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
from classes import pcdm

with open('logging.yml', 'r') as configfile:
    logging_config = yaml.safe_load(configfile)
    logging.config.dictConfig(logging_config)

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

def load_item(fcrepo, item, args):
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

        if args.extra:
            logger.info('Adding additional triples')
            if re.search(r'\.(ttl|n3|nt)$', args.extra):
                rdf_format = 'n3'
            elif re.search(r'\.(rdf|xml)$', args.extra):
                rdf_format = 'xml'
            item.add_extra_properties(args.extra, rdf_format)

        logger.info('Updating item and components')
        item.recursive_update(fcrepo, args.nobinaries)

        # commit transaction
        logger.info('Committing transaction')
        fcrepo.commit_transaction()

    except pcdm.RESTAPIException as e:
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
    print_header()

    parser = argparse.ArgumentParser(
        description='A configurable batch loader for Fedora 4.'
        )

    '''TODO: Config should support storage of a list of WebAC ACL URIs
       according to the most common access control policies in use by the
       repository, so the data handler can access and apply them.'''

    # Path to the repo config (endpoint, relpath, credentials, and WebAC paths)
    parser.add_argument('-c', '--config',
                        help='Path to configuration file.',
                        action='store',
                        required=True
                        )

    # Data handler module to use
    parser.add_argument('-H', '--handler',
                        help='Data handler module to use.',
                        action='store',
                        required=True
                        )

    # The mapfile records path and URI of successfully created items.
    # Transactions prevent orphan resource creation below the item level.
    # Items in an existing map file are skipped upon subsequent runs.
    parser.add_argument('-m', '--map',
                        help='Mapfile to store results of load.',
                        action='store',
                        default="logs/mapfile.csv"
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

    # Extra triples to add to each item
    parser.add_argument('-x', '--extra',
                        help='''File containing extra triples to add to each
                                item''',
                        action='store'
                        )

    # Path to the data set (metadata and files)
    parser.add_argument('path',
                        help='Path to data set to be loaded.',
                        action='store'
                        )

    args = parser.parse_args()

    # Load config
    if args.config:
        with open(args.config, 'r') as configfile:
            config = yaml.safe_load(configfile)
            fcrepo = pcdm.Repository(config)
            logger.info('Loaded configuration from {0}'.format(args.config))
    else:
        logger.warn('No configuration file specified.')

    # "--ping" tests repository connection and exits
    if args.ping:
        test_connection(fcrepo)
        sys.exit(0)

    # Define the specified data_handler function for the data being loaded
    logger.info("Initializing data handler")
    if args.handler:
        handler = import_module('handler.' + args.handler)
        logger.info('Loaded "{0}" handler'.format(args.handler))
    else:
        logger.warn('No data handler specified.')

    # The handler is always invoked by calling the load function defined in
    # the specified handler on a specified local path (file or directory).
    batch = handler.load(args)

    if not args.dryrun:
        test_connection(fcrepo)

        # open mapfile, if it exists, and read completed files into list
        fieldnames = ['number', 'timestamp', 'title', 'path', 'uri']
        completed_items = []
        skip_list = []
        if os.path.isfile(args.map):
            with open(args.map, 'r') as infile:
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
        with open(args.map, 'w+') as mapfile:
            writer = csv.DictWriter(mapfile, fieldnames=fieldnames)
            writer.writeheader()
            # write out completed items
            logger.info('Writing data for {0} existing items to mapfile.'.format(
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

                logger.info("Processing item {0}/{1}...".format(n+1, batch.length))
                item.print_item_tree()

                try:
                    logger.info('Loading item {0}'.format(n+1))
                    load_item(fcrepo, item, args)
                except pcdm.RESTAPIException as e:
                    logger.error("Unable to commit or rollback transaction, aborting")
                    sys.exit(1)

                # write item details to mapfile
                row = {'number': n + 1,
                       'timestamp': item.creation_timestamp,
                       'title': item.title,
                       'path': item.path,
                       'uri': item.uri
                       }
                writer.writerow(row)

    print_footer()

if __name__ == "__main__":
    main()
