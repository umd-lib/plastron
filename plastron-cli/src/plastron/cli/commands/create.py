import logging
from argparse import Namespace
from pathlib import Path

from rdflib import Graph, Literal, URIRef

from plastron.cli import parse_data_property, parse_object_property
from plastron.cli.commands import BaseCommand
from plastron.namespaces import dcterms, get_manager, pcdm, rdf
from plastron.utils import uri_or_curie

logger = logging.getLogger(__name__)

manager = get_manager()


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='create',
        description='Create a resource in the repository'
    )
    parser.add_argument(
        '-D', '--data-property',
        help=(
            'an RDF data property to set on the newly created resource; '
            'VALUE is treated as a Literal; repeatable'
        ),
        action='append',
        nargs=2,
        dest='data_properties',
        metavar=('PREDICATE', 'VALUE'),
        default=[]
    )
    parser.add_argument(
        '-O', '--object-property',
        help=(
            'an RDF object property to set on the newly created resource; '
            'VALUE is treated as a CURIE or URIRef; repeatable'
        ),
        action='append',
        nargs=2,
        dest='object_properties',
        metavar=('PREDICATE', 'VALUE'),
        default=[]
    )
    parser.add_argument(
        '-T', '--rdf-type',
        help=(
            'RDF type to add to the newly created resource; equivalent to '
            '"-O rdf:type TYPE"; TYPE is treated as a CURIE or URIRef; '
            'repeatable'
        ),
        action='append',
        dest='types',
        metavar='TYPE',
        default=[]
    )
    parser.add_argument(
        '--collection',
        help='shortcut for "-T pcdm:collection -D dcterms:title NAME"',
        metavar='NAME',
        action='store',
        dest='collection_name'
    )
    container_or_path = parser.add_mutually_exclusive_group(required=True)
    container_or_path.add_argument(
        'path',
        nargs='?',
        help='path to the new resource',
        action='store'
    )
    container_or_path.add_argument(
        '--container',
        help=(
            'parent container for the new resource; use this to create a new '
            'resource with a repository-generated identifier'
        ),
        metavar='PATH',
        action='store'
    )
    parser.set_defaults(cmd_name='create')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        properties: list[tuple[URIRef, Literal | URIRef]] = [
            *(parse_data_property(p, o) for p, o in args.data_properties),
            *(parse_object_property(p, o) for p, o in args.object_properties)
        ]

        if args.collection_name is not None:
            properties.append((rdf.type, pcdm.Collection))
            properties.append((dcterms.title, Literal(args.collection_name)))

        for rdf_type in args.types:
            properties.append((rdf.type, uri_or_curie(rdf_type)))

        graph = Graph(namespace_manager=manager)
        for p, o in properties:
            graph.add((URIRef(''), p, o))

        if args.path is not None:
            self.context.client.create_at_path(Path(args.path), graph)
        elif args.container is not None:
            self.context.client.create_in_container(Path(args.container), graph)
