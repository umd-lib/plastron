#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import argparse
import sys
import yaml
import logging
import logging.config
from datetime import datetime
from classes import pcdm

logger = logging.getLogger(__name__)

def main():
    '''Parse args and handle options.'''

    parser = argparse.ArgumentParser(
        description='Recursive object lister for Fedora 4.'
        )

    # Path to the repo config (endpoint, relpath, credentials, and WebAC paths)
    parser.add_argument('-r', '--repo',
                        help='Path to repository configuration file.',
                        action='store',
                        required=True
                        )

    args = parser.parse_args()

    # configure logging
    with open('config/logging.yml', 'r') as configfile:
        logging_config = yaml.safe_load(configfile)
        logfile = 'logs/read.py.{0}.log'.format(
            datetime.utcnow().strftime('%Y%m%d%H%M%S')
            )
        logging_config['handlers']['file']['filename'] = logfile
        logging_config['handlers']['console']['stream'] = 'ext://sys.stderr'
        logging.config.dictConfig(logging_config)

    # Load required repository config file and create repository object
    with open(args.repo, 'r') as repoconfig:
        fcrepo = pcdm.Repository(yaml.safe_load(repoconfig))
        logger.info('Loaded repo configuration from {0}'.format(args.repo))

    predicates = [pcdm.pcdm.hasMember, pcdm.pcdm.hasFile, pcdm.pcdm.hasRelatedObject]
    for item_uri in sys.stdin:
        for uri in fcrepo.recursive_get(item_uri.rstrip('\n'), traverse=predicates):
            print(uri)

if __name__ == "__main__":
    main()
