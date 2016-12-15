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

from classes import pcdm



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

    # Path to the repo config (endpoint, credentials, and WebAC paths)
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
                        default="mapfile.csv"
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
                        help='File containing extra triples to add to each item',
                        action='store'
                        )

    # Path to the data set (metadata and files)
    parser.add_argument('path',
                        help='Path to data set to be loaded.',
                        action='store'
                        )

    args = parser.parse_args()

    # Load config
    print("Configuring repo connection...", end='')
    if args.config:
        with open(args.config, 'r') as configfile:
            config = yaml.safe_load(configfile)
            fcrepo = pcdm.Repository(config)
            print(' loading {0} => Done!'.format(args.config))
    else:
        print(' no configuration specified.')

    # "--ping" tests repository connection and exits
    if args.ping:
        test_connection(fcrepo)
        exit(0)

    # Define the specified data_handler function for the data being loaded
    print("Initializing data handler...", end='')
    if args.handler:
        handler = import_module('handler.' + args.handler)
        print(' loading "{0}" handler => Done!'.format(args.handler))
    else:
        print(' no data handler specified.')

    # The handler is always invoked by calling the load function defined in
    # the specified handler on a specified local path (file or directory).
    batch = handler.load(args)
    batch.print_tree()

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
                    print('ERROR: Non-standard map file specified!')
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
            for row in completed_items:
                writer.writerow(row)
                print('Writing data for {0} existing items to mapfile.'.format(
                        len(completed_items)
                        )
                     )

            # create all batch objects in repository
            for n, item in enumerate(batch.items):
                if args.limit is not None and n >= args.limit:
                    print("Stopping after {0} item(s)".format(args.limit))
                    break
                elif item.path in skip_list:
                    continue

                print('\nLoading item {0}...'.format(n+1))
                item.recursive_create(fcrepo, args.nobinaries)
                print('\nCreating ordering proxies ...')
                item.create_ordering(fcrepo)
                print('\nUpdating relationship triples ...')
                item.update_relationship_triples()

                if args.extra:
                    print('\nAdding additional triples ...')
                    if re.search(r'\.(ttl|n3|nt)$', args.extra):
                        rdf_format = 'n3'
                    elif re.search(r'\.(rdf|xml)$', args.extra):
                        rdf_format = 'xml'
                    item.add_extra_properties(args.extra, rdf_format)

                print('\nUpdating item {0}...'.format(n+1))
                item.recursive_update(fcrepo, args.nobinaries)

                # write item details to mapfile
                row = {'number': n + 1,
                       'timestamp': item.creation_timestamp,
                       'title': item.title,
                       'path': item.path,
                       'uri': item.uri
                       }
                writer.writerow(row)

    print_footer()

def test_connection(fcrepo):
    # test connection to fcrepo
    print("Testing connection to {0}".format(fcrepo.endpoint),
            file=sys.stderr)
    if fcrepo.is_reachable():
        print("Connection successful.", file=sys.stderr)
    else:
        print("Unable to connect.", file=sys.stderr)
        exit(1)

if __name__ == "__main__":
    main()
