#!/usr/bin/env python3

import argparse
import sys
import yaml
import logging
import logging.config
from datetime import datetime
from classes import pcdm,ocr
from handler import ndnp
import rdflib
from rdflib import RDF
from lxml import etree as ET

logger = logging.getLogger(__name__)

# stub page object
class Page(object):
    def __init__(self, uri, ocr_resource, ocr_file):
        self.uri = uri
        self.ocr = ocr_resource
        self.ocr_file = ocr_file

# sub file object
class File(object):
    def __init__(self, uri):
        self.uri = uri

def main():
    '''Parse args and handle options.'''

    parser = argparse.ArgumentParser(
        description='Extract OCR text and create annotations.'
        )

    # Path to the repo config (endpoint, relpath, credentials, and WebAC paths)
    parser.add_argument('-r', '--repo',
                        help='Path to repository configuration file.',
                        action='store',
                        required=True
                        )

    parser.add_argument('uris', nargs='+',
                        help='One or more repository URIs to extract text from.'
                        )

    args = parser.parse_args()

    # configure logging
    with open('config/logging.yml', 'r') as configfile:
        logging_config = yaml.safe_load(configfile)
        logfile = 'logs/extractocr.py.{0}.log'.format(
            datetime.utcnow().strftime('%Y%m%d%H%M%S')
            )
        logging_config['handlers']['file']['filename'] = logfile
        logging.config.dictConfig(logging_config)

    # Load required repository config file and create repository object
    with open(args.repo, 'r') as repoconfig:
        fcrepo = pcdm.Repository(yaml.safe_load(repoconfig))
        logger.info('Loaded repo configuration from {0}'.format(args.repo))

    with fcrepo.at_path('/annotations'):
        for issue_uri in args.uris:
            fcrepo.open_transaction()
            issue_graph = fcrepo.get_graph(issue_uri)
            issue_uri = rdflib.URIRef(fcrepo._insert_transaction_uri(issue_uri))
            for member_uri in issue_graph.objects(subject=issue_uri, predicate=pcdm.pcdm.hasMember):
                member_graph = fcrepo.get_graph(member_uri)
                if (member_uri, RDF.type, ndnp.ndnp.Page) in member_graph:
                    page = ndnp.Page.from_repository(fcrepo, member_uri, graph=member_graph)
                    logger.info("Creating annotations for page {0}".format(page.title))
                    for annotation in page.textblocks():
                        annotation.create_object(fcrepo)
                        annotation.update_object(fcrepo)
            fcrepo.commit_transaction()


if __name__ == "__main__":
    main()
