#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import argparse
import urllib2
import rdflib
import os
import sys
import yaml


def load_config(configfile):
    print('Loading configuration from "{0}" ... '.format(configfile), end='')
    with open(schemafile, 'r') as f:
        config = yaml.load(f)
    print("success.")
    return config


def main():
    '''Parse args and run batch load.'''
    parser = argparse.ArgumentParser(
        description='Newspaper Batch Loader for Fedora 4.'
        )
    parser.add_argument("config")
    parser.add_argument("path")
    
    args = parser.parse_args()



if __name__ == "__main__":
    main()
