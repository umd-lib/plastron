#!/usr/bin/env python3

'''Indexing verification tool for fcrepo: reads items loaded from batch
   configuration file, queries the appropriate solr server, and reports 
   missing items.
   
   Fedora Indexing Verification Tool.
   
   optional arguments:
   -h, --help               show this help message and exit
   -b BATCH, --batch BATCH  Path to batch configuration file.
   -r REPO, --repo REPO     Path to repository configuration file.'''

import argparse
import csv
import json
import os
import requests
import sys
from urllib.parse import urlparse
import yaml

'''Parse args and verify loaded items.'''
parser = argparse.ArgumentParser(
    description='Fedora Indexing Verification Tool.'
    )

# Path to the loader config 
parser.add_argument('-b', '--batch',
                    help='Path to batch configuration file.',
                    action='store',
                    required=True
                    )

# Path to the loader config 
parser.add_argument('-r', '--repo',
                    help='Path to repository configuration file.',
                    action='store',
                    required=True
                    )

args = parser.parse_args()

# Load required batch config file
with open(args.batch, 'r') as batch_config:
    batch = yaml.safe_load(batch_config)
    print('Loaded batch configuration from {0}'.format(args.batch))

# Load required repo config file
with open(args.repo, 'r') as repo_config:
    repo = yaml.safe_load(repo_config)
    print('Loaded repo configuration from {0}'.format(args.repo))

# Read uris for items loaded to fcrepo from mapfile
mapfile_path = os.path.join(batch['LOG_LOCATION'], batch['MAPFILE'])
with open(mapfile_path, 'r') as mapfile:
    reader = csv.DictReader(mapfile)
    fcrepo_uris = set([row['uri'] for row in reader])
    print('Loaded {0} uris from {1}'.format(len(fcrepo_uris), mapfile_path))

# read repo config info and construct query to appropriate solr server
host = urlparse(repo['REST_ENDPOINT']).netloc
subdomain = host.split('.')[0]
server_level = subdomain.lstrip('fcrepo')
solr_server = 'https://solr{0}.lib.umd.edu'.format(server_level)
query = solr_server + \
    '/solr/fedora4/select?fl=id&q=component:issue&rows=5000&wt=json'

# query the relevant solr server for all ids
response = requests.get(query)
solr_uris = set(
    [i['id'] for i in json.loads(response.text)['response']['docs']]
    )
print('Retrieved {0} uris from {1}'.format(len(solr_uris), solr_server))

# determine while fcrepo uris are not in solr
not_solr = fcrepo_uris - solr_uris

# report results
print('The following {0} items have not been indexed:'.format(len(not_solr)))
for uri in not_solr: print(uri)
