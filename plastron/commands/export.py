import logging

logger = logging.getLogger(__name__)

def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='export',
        description='Export resources from the repository'
    )
    parser.add_argument(
        '-n', '--name',
        help='Export job name',
        action='store',
        required=True
    )
    parser.add_argument(
        'uris',
        nargs='*',
        help='URIs of repository objects to export'
    )
    parser.set_defaults(cmd_name='export')

class Command:
    def __call__(self, fcrepo, args):
        count = 0
        total = len(args.uris)
        with open(f'export-{args.name}.ttl', 'wb') as fh:
            for uri in args.uris:
                r = fcrepo.head(uri)
                if r.status_code == 200:
                    # do export
                    logger.info(f'Exporting item {count + 1}/{total}: {uri}')
                    graph = fcrepo.get_graph(uri)
                    graph.serialize(destination=fh, format='turtle')
                    count += 1
        logger.info(f'Exported {count} of {total} items')
