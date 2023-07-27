import logging
from argparse import Namespace
from pathlib import Path
from typing import List, Tuple, Union

from rdflib import Graph, Literal, URIRef

from plastron.client import Client
from plastron.commands import BaseCommand
from plastron.namespaces import dcterms, get_manager, pcdm, rdf
from plastron.rdf import parse_data_property, parse_object_property, uri_or_curie

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


def paths_to_create(client: Client, path: Path) -> List[Path]:
    if client.path_exists(str(path)):
        return []
    to_create = [path]
    for ancestor in path.parents:
        if not client.path_exists(str(ancestor)):
            to_create.insert(0, ancestor)
    return to_create


def serialize(graph: Graph, **kwargs):
    logger.info('Including properties:')
    for _, p, o in graph:
        logger.info(f'  {p.n3(namespace_manager=manager)} {o.n3(namespace_manager=manager)}')
    return graph.serialize(**kwargs)


class Command(BaseCommand):
    def __call__(self, client: Client, args: Namespace):
        self.repo = client

        properties: List[Tuple[URIRef, Union[Literal, URIRef]]] = [
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
            self.create_at_path(Path(args.path), graph)
        elif args.container is not None:
            self.create_in_container(Path(args.container), graph)

    def create_at_path(self, target_path: Path, graph: Graph = None):
        all_paths = paths_to_create(self.repo, target_path)

        if len(all_paths) == 0:
            logger.info(f'{target_path} already exists')
            return

        resource = None
        for path in all_paths:
            logger.info(f'Creating {path}')
            if path == target_path and graph:
                resource = self.repo.create(
                    path=str(path),
                    headers={
                        'Content-Type': 'text/turtle'
                    },
                    data=serialize(graph, format='turtle')
                )
            else:
                resource = self.repo.create(path=str(path))

            logger.info(f'Created {resource}')

        return resource

    def create_in_container(self, container_path: Path, graph: Graph = None):
        if not self.repo.path_exists(str(container_path)):
            logger.error(f'Container path "{container_path}" not found')
            return
        if graph:
            resource = self.repo.create(
                container_path=str(container_path),
                headers={
                    'Content-Type': 'text/turtle'
                },
                data=serialize(graph, format='turtle')
            )
        else:
            resource = self.repo.create(container_path=str(container_path))

        logger.info(f'Created {resource}')
        return resource
