#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import logging
import lxml.etree as ET
import os
import rdflib
import requests
import sys
import yaml


#============================================================================
# HELPER FUNCTIONS 
#============================================================================

def print_header():
    '''Common header formatting.'''
    title = 'FCREPO BATCH LOADER'
    bar = '=' * len(title)
    print('\n'.join([" ", title, bar]))


#============================================================================
# CLASSES
#============================================================================

class batch():
    '''class representing the set of resources to be loaded'''
    def __init__(self, batchfile):
        tree = ET.parse(batchfile)
        root = tree.getroot()
        self.basepath = os.path.dirname(batchfile)
        self.issues = root.findall('./{http://www.loc.gov/ndnp}issue')
        print("Batch contains {} issues: ".format(len(self.issues)))
        for n, issue in enumerate(self.issues):
            print("  {}. ".format(n+1), end='')
            i = item(os.path.join(self.basepath, issue.text))
            

class item():
    '''class representing the files of an individual item'''
    def __init__(self, path):
        tree = ET.parse(path)
        root = tree.getroot()
        searchpath = ("{http://www.loc.gov/METS/}fileSec/"
                      "{http://www.loc.gov/METS/}fileGrp")
        self.pages = root.findall(searchpath)
        print("Issue contains {} pages.".format(len(self.pages)))
        
        for p in self.pages:
            for child in p:
                print(child[0])
                for k,v in child[0].items():
                    print(v)
                
#            page(p)
            
        '''
        self.volume = 
        self.issue = 
        self.edition = 
        '''


class page():
    '''class representing the individual page (fileset)'''
    def __init__(self, source):
        tree = ET.parse(source)
        root = tree.getroot()
        print(root)


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
        print("Successfully loaded configuration information.\n")
    
    b = batch(args.path)


if __name__ == "__main__":
    main()
