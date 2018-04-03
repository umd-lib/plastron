import logging
from classes.util import get_title_string, parse_predicate_list

logger = logging.getLogger(__name__)

def run(fcrepo, args):
    if args.recursive is not None:
        args.predicates = parse_predicate_list(args.recursive)
        logger.info('Listing will traverse the following predicates: {0}'.format(
            ', '.join([ p.n3() for p in args.predicates ]))
            )
    else:
        args.predicates = []

    for item_uri in args.uris:
        for (uri, graph) in fcrepo.recursive_get(item_uri, traverse=args.predicates):
            if args.long:
                title = get_title_string(graph)
                print("{0} {1}".format(uri, title))
            else:
                print(uri)
