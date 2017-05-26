#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import argparse
import sys
import yaml
import logging
import logging.config
from datetime import datetime
from classes import pcdm
import rdflib

logger = logging.getLogger(__name__)

def main():
    '''Parse args and handle options.'''

    parser = argparse.ArgumentParser(
        description='Object lister for Fedora 4.'
        )

    # Path to the repo config (endpoint, relpath, credentials, and WebAC paths)
    parser.add_argument('-r', '--repo',
                        help='Path to repository configuration file.',
                        action='store',
                        required=True
                        )

    # long mode to print more than just the URIs (name modeled after ls -l)
    parser.add_argument('-l', '--long',
                        help='Display additional information besides the URI',
                        action='store_true'
                        )

    parser.add_argument('-R', '--recursive',
                        help='List additional objects found by traversing the given predicate(s)',
                        action='store'
                        )

    args = parser.parse_args()

    # configure logging
    with open('config/logging.yml', 'r') as configfile:
        logging_config = yaml.safe_load(configfile)
        logfile = 'logs/list.py.{0}.log'.format(
            datetime.utcnow().strftime('%Y%m%d%H%M%S')
            )
        logging_config['handlers']['file']['filename'] = logfile
        logging_config['handlers']['console']['stream'] = 'ext://sys.stderr'
        logging.config.dictConfig(logging_config)

    # Load required repository config file and create repository object
    with open(args.repo, 'r') as repoconfig:
        fcrepo = pcdm.Repository(yaml.safe_load(repoconfig))
        logger.info('Loaded repo configuration from {0}'.format(args.repo))

    if args.recursive is not None:
        args.predicates = [
            rdflib.util.from_n3(p, nsm=pcdm.namespace_manager)
            for p in args.recursive.split(',')
            ]
        logger.info('Listing will traverse the following predicates: {0}'.format(
            ', '.join([ p.n3() for p in args.predicates ]))
            )
    else:
        args.predicates = []

    for item_uri in sys.stdin:
        for (uri, graph) in fcrepo.recursive_get(item_uri.rstrip('\n'), traverse=args.predicates):
            title = '; '.join([ t for t in graph.objects(predicate=pcdm.dcterms.title) ])
            if args.long:
                print("{0} ({1})".format(uri, title))
            else:
                print(uri)

if __name__ == "__main__":
    main()
