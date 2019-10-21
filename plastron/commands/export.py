import logging
from time import sleep

from plastron.exceptions import ConfigException, DataReadException
from plastron.namespaces import get_manager
from plastron.serializers import SERIALIZER_CLASSES

logger = logging.getLogger(__name__)
nsm = get_manager()


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='export',
        description='Export resources from the repository'
    )
    parser.add_argument(
        '-o', '--output-file',
        help='File to write export package to',
        action='store',
    )
    parser.add_argument(
        '-f', '--format',
        help='Export job format',
        action='store',
        choices=SERIALIZER_CLASSES.keys(),
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
        try:
            serializer_class = SERIALIZER_CLASSES[args.format]
        except KeyError:
            raise ConfigException(f'Unknown format: {args.format}')

        logger.debug(f'Exporting to file {args.output_file}')
        with serializer_class(args.output_file) as serializer:
            for uri in args.uris:
                r = fcrepo.head(uri)
                if r.status_code == 200:
                    # do export
                    if 'describedby' in r.links:
                        # the resource is a binary, get the RDF description URI
                        rdf_uri = r.links['describedby']['url']
                    else:
                        rdf_uri = uri
                    logger.info(f'Exporting item {count + 1}/{total}: {uri}')
                    graph = fcrepo.get_graph(rdf_uri)
                    try:
                        serializer.write(graph)
                        count += 1
                    except DataReadException as e:
                        # log the failure, but continue to attempt to export the rest of the URIs
                        logger.error(f'Export of {uri} failed: {e}')
                    sleep(1)

        logger.info(f'Exported {count} of {total} items')
        return {
            'content_type': serializer.content_type,
            'file_extension': serializer.file_extension,
            'count': {
                'total': total,
                'exported': count,
                'skipped': total - count
            }
        }
