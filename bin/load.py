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

import metadata_map

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
        md = metadata_map.batch
        
        self.basepath = os.path.dirname(batchfile)
        self.issues = root.findall(md['issues'])
        print("Batch contains {} issues: ".format(len(self.issues)))
        for n, issue in enumerate(self.issues):
            print("\n\n ISSUE {}: ".format(n+1))
            i = item(os.path.join(self.basepath, issue.text))
            
            

class item():
    '''class representing the files of an individual item'''
    def __init__(self, path):
        tree = ET.parse(path)
        root = tree.getroot()
        m = metadata_map.issue
        
        self.dir     = os.path.dirname(path)
        self.volume  = root.find(m['volume']).text
        self.issue   = root.find(m['issue']).text
        self.edition = root.find(m['edition']).text
        self.date    = root.find(m['date']).text
        self.pages   = [
            p for p in root.findall(m['pages']) \
                if p.get('ID').startswith('pageModsBib')
            ]
        self.files   = [f for f in root.findall(m['files'])]
        
        header = "Diamondback {0}: Vol. {1}.{2} (ed. {3})".format(
            self.date, self.volume, self.issue, self.edition
            )
        border = "=" * (len(header) + 4)
        
        print(border)
        print("|", header, "|")
        print(border)
        
        for n, pagexml in enumerate(self.pages):
            id = pagexml.get('ID').strip('pageModsBib')
            filegroup = next(
                f for f in self.files if f.get('ID').endswith(id)
                )
            p = page(pagexml, filegroup)

            print("  Page {0}: Reel {1}, Frame {2}".format(
                n+1, p.reel, p.frame
                ))

            for f in p.files:
                f.path = os.path.join(self.dir, os.path.basename(f.relpath))
                print('   => {0}: {1}'.format(f.use, f.path))



class page():
    '''class representing the individual page'''
    def __init__(self, pagexml, filegroup):
        m = metadata_map.page
        self.reel   = pagexml.find(m['reel']).text
        self.frame  = pagexml.find(m['frame']).text
        self.files  = [file(f) for f in filegroup.findall(m['files'])]
        
               
        
class file():
    '''class representing the individual file'''
    def __init__(self, filexml):
        m = metadata_map.file
        self.use  = filexml.get('USE')
        elem = filexml.find(m['filepath'])
        self.relpath = elem.get('{http://www.w3.org/1999/xlink}href')


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


if __name__ == "__main__":
    main()
