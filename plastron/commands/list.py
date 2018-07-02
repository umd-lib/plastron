import logging
from plastron.util import get_title_string, parse_predicate_list

logger = logging.getLogger(__name__)

class Command:
    def __init__(self, subparsers):
        parser_list = subparsers.add_parser('list', aliases=['ls'],
                description='List objects in the repository')
        # long mode to print more than just the URIs (name modeled after ls -l)
        parser_list.add_argument('-l', '--long',
                            help='Display additional information besides the URI',
                            action='store_true'
                            )
        parser_list.add_argument('-R', '--recursive',
                            help='List additional objects found by traversing the given predicate(s)',
                            action='store'
                            )
        parser_list.add_argument('uris', nargs='*',
                            help='URIs of repository objects to list'
                            )
        parser_list.set_defaults(cmd_name='list')

    def __call__(self, fcrepo, args):
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
