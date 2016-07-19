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


class batch(path):
    '''class representing the set of resources to be loaded'''
    def __init__(self, path):
        self.parse(filepath, format='turtle')
        self.filename = os.path.basename(filepath)
        self.uri = uri
        print(" - Resource {0} has {1} triples.".format(
            self.filename, len(self))
            )
    




def main():
    '''Parse args and run batch load.'''
    parser = argparse.ArgumentParser(
        description='Newspaper Batch Loader for Fedora 4.'
        )
    parser.add_argument("--config", "-c", action="store")
    parser.add_argument("path")
    
    args = parser.parse_args()

    with open(args.config, 'r') as configfile:
        globals().update(yaml.safe_load(configfile))


if __name__ == "__main__":
    main()
