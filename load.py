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


def load_batch():
    b = batch(args.path)
    
    for n, item in enumerate(b.items):
        print("\n\n ISSUE {}: ".format(n+1))
        header = "Diamondback {0}: Vol. {1}.{2} (ed. {3})".format(
            item.date, item.volume, item.issue, item.edition
            )
        border = "=" * (len(header) + 4)
        
        print(border)
        print("|", header, "|")
        print(border)
        
        for n, page in enumerate(item.pages):
            print("  Page {0}: Reel {1}, Frame {2}".format(
                n+1, page.reel, page.frame
                ))
            for file in page.files:
                print('   => {0}: {1}'.format(file.use, file.path))
        
        print("\nCreating empty repository container...")
        response = requests.post(REST_ENDPOINT)
        if response.status_code == 201:
            uri = response.text
            print("  {0} Created! => {1}".format(response, uri))
        
        print("\nBuilding RDF Graph...")
        properties = rdflib.Graph()


#============================================================================
# MAIN LOOP
#============================================================================

def main():
    '''Parse args and handle options.'''
    print_header()
    
    parser = argparse.ArgumentParser(
        description='A configurable batch loader for Fedora 4.'
        )
    
    # Path to the repo config (endpoint, credentials, and WebAC paths)
    
    '''TODO: Config should support storage of a list of WebAC ACL URIs 
       according to the most common access control policies in use by the 
       repository, so the data handler can access and apply them.''' 
    
    parser.add_argument('-c', '--config', 
                        help='Path to configuration file.',
                        action='store'
                        )
    
    # Data handler module to use
    parser.add_argument('-H', '--handler', 
                        help='Data handler module to use.',
                        action='store'
                        )
    
    # Support resuming interrupted jobs by storing URIs for completed items
    
    '''TODO: Loader needs to save resource ID and URI to a map file, and also
       support reading an existing map file and skipping previously created
       resources; because URIs are created first, the loader should also 
       support PATCHing existing URIs that have incomplete resource objects
       attached to them; or perhaps better, should use transactions to prevent
       incomplete resources being created.''' 
       
    parser.add_argument('-r', '--resume', 
                        help='Resume interrupted batch using results file.',
                        action='store'
                        )
    
    parser.add_argument('-d', '--dryrun', 
                        help='Iterate over the batch without POSTing.',
                        action='store_true'
                        )
    
    # Path to the data set (metadata and files)
    parser.add_argument('path', 
                        help='Path to data set to be loaded.',
                        action='store'
                        )
    
    args = parser.parse_args()
    
    
    # Load config and check repository connection
    print("Configuring repo connection...", end='')
    if args.config:
        with open(args.config, 'r') as configfile:
            globals().update(yaml.safe_load(configfile))
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
    
    b = handler.load(args.path)
    
    for item in b.items:
        resource = pcdm.ItemObj(item)
        
        if args.dryrun:
            resource.uri = resource.path
        else:
            resource.get_uri(REST_ENDPOINT, FEDORA_USER, FEDORA_PASSWORD)
        
        resource.add_metadata_to_graph()
        
        response = resource.deposit(FEDORA_USER, FEDORA_PASSWORD)
        
        print(response)
        
    print_footer()
    

if __name__ == "__main__":
    main()
