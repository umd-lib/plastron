#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import logging
import rdflib
import requests
import sys
import yaml

from ndnp import *

#============================================================================
# HELPER FUNCTIONS 
#============================================================================

def print_header():
    '''Common header formatting.'''
    title = 'FCREPO BATCH LOADER'
    bar = '=' * len(title)
    print('\n'.join([" ", title, bar]))


#============================================================================
# MAIN LOOP
#============================================================================

def main():
    '''Parse args and run batch load.'''
    print_header()
    
    parser = argparse.ArgumentParser(
        description='Newspaper Batch Loader for Fedora 4.'
        )
    parser.add_argument("--config", "-c", action="store")
    parser.add_argument("path")
    args = parser.parse_args()

    with open(args.config, 'r') as configfile:
        globals().update(yaml.safe_load(configfile))
        print("Successfully loaded configuration information.")
        print("Successfully imported metadata mapping.")
    
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
        
        print("\nCreating LDP Container...")
        response = requests.post(REST_ENDPOINT)
        if response.status_code == 201:
            uri = response.text
            print("  {0} Created! => {1}".format(response, uri))
        
        print("\nCreating RDF Graph...")
        props = rdflib.Graph()
        
        
        
        '''
        Create collection (c) and set up ACL
            <> a PCDM:Collection
            
        For each item (i) in collection:
            c hasMember i
            <> a pcdm:Object
            <> pcdm:memberOf c
            
            for each page (p) in i
                <> a pcdm:Object
                
                for each file (f) in p
                    <> a pcdm:file
                    <> pcdm:fileOf
                
                    attach binary to pcdm:File
        
        '''
        



if __name__ == "__main__":
    main()
