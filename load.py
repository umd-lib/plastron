#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import logging
from importlib import import_module
import pprint
import rdflib
import requests
import sys
import yaml

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

    '''TODO: Loader needs to save resource ID and URI to a map file, and also
       support reading an existing map file and skipping previously created
       resources; because URIs are created first, the loader should also
       support PATCHing existing URIs that have incomplete resource objects
       attached to them; or perhaps better, should use transactions to prevent
       incomplete resources being created.'''

    ''' # Support resuming interrupted jobs by storing URIs for completed items
        # NOT YET IMPLEMENTED
    parser.add_argument('-r', '--resume',
                        help='Resume interrupted batch using results file.',
                        action='store'
                        ) '''

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

    # Path to the data set (metadata and files)
    parser.add_argument('path',
                        help='Path to data set to be loaded.',
                        action='store'
                        )

    # Limit the load to a specified number of top-level objects
    parser.add_argument('-l', '--limit',
                        help='Limit the load to a specified number of top-level objects.',
                        action='store',
                        type=int,
                        default=None
                        )


    args = parser.parse_args()


    # Load config and check repository connection
    print("Configuring repo connection...", end='')
    if args.config:
        with open(args.config, 'r') as configfile:
            config = yaml.safe_load(configfile)
            auth = (config['FEDORA_USER'], config['FEDORA_PASSWORD'])
            fcrepo = pcdm.Repository(config['REST_ENDPOINT'], auth)
            print(' loading {0} => Done!'.format(args.config))
    else:
        print(' no configuration specified.')


    # Define the specified data_handler function for the data being loaded
    print("Initializing data handler...", end='')
    if args.handler:
        handler = import_module('handler.' + args.handler)
        print(' loading "{0}" handler => Done!'.format(args.handler))
    else:
        print(' no data handler specified.')


    # The handler is always invoked by calling the load function defined in
    # the specified handler on a specified local path (file or directory).
    batch = handler.load(args.path)
    batch.print_tree()

    if not args.dryrun:
        # create all batch objects in repository
        for n, item in enumerate(batch.items):
            if args.limit is not None and n >= args.limit:
                print("Stopping after {0} item(s)".format(args.limit))
                break

            print('\nLoading item {0}...'.format(n+1))
            item.recursive_create(fcrepo, args.nobinaries)
            print('\nCreating ordering proxies ...')
            item.create_ordering(fcrepo)
            print('\nUpdating relationship triples ...')
            item.update_relationship_triples()
            print('\nUpdating item {0}...'.format(n+1))
            item.recursive_update(fcrepo, args.nobinaries)


    print_footer()


if __name__ == "__main__":
    main()
