import logging
from argparse import Namespace
from typing import Callable, Iterable, Iterator

from rdflib import Literal, URIRef

from plastron.cli import parse_data_property, parse_object_property
from plastron.cli.commands import BaseCommand
from plastron.namespaces import get_manager, rdf
from plastron.repo import RepositoryResource
from plastron.utils import parse_predicate_list, uri_or_curie

logger = logging.getLogger(__name__)
manager = get_manager()


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='find',
        description='Find objects in the repository'
    )
    parser.add_argument(
        '-R', '--recursive',
        help='search additional objects found by traversing the given predicate(s)',
        action='store'
    )
    parser.add_argument(
        '-D', '--data-property',
        help=(
            'an RDF data property to match; '
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
            'an RDF object property to match; '
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
            'RDF type to match; equivalent to "-O rdf:type TYPE"; '
            'TYPE is treated as a CURIE or URIRef; repeatable'
        ),
        action='append',
        dest='types',
        metavar='TYPE',
        default=[]
    )
    any_or_all = parser.add_mutually_exclusive_group()
    any_or_all.add_argument(
        '--match-all',
        help=(
            'require all properties to match to include a resource in the result list; '
            'this is the default behavior'
        ),
        default=False,
        action='store_true'
    )
    any_or_all.add_argument(
        '--match-any',
        help='require at least one property to match to include a resource in the result list',
        default=False,
        action='store_true'
    )
    parser.add_argument(
        'uris', nargs='*',
        metavar='URI',
        help='search at this URI in the repository'
    )
    parser.set_defaults(cmd_name='find')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        self.properties: list[tuple[URIRef, Literal | URIRef]] = [
            *(parse_data_property(p, o) for p, o in args.data_properties),
            *(parse_object_property(p, o) for p, o in args.object_properties)
        ]

        for rdf_type in args.types:
            self.properties.append((rdf.type, uri_or_curie(rdf_type)))

        if args.match_any:
            self.match = any
        else:
            self.match = all

        if len(self.properties) == 0:
            logger.info('Matching all resources')
        else:
            logger.info(f'Matching {self.match.__name__} of these properties:')
            for p, o in self.properties:
                logger.info(f'  {p.n3(namespace_manager=manager)} {o.n3(namespace_manager=manager)}')

        self.resource_count = 0

        traverse = parse_predicate_list(args.recursive) if args.recursive else []

        if len(traverse) != 0:
            logger.info('Predicates used for recursive matching:')
            for p in traverse:
                logger.info(f'  {p.n3(namespace_manager=manager)}')

        for uri in args.uris:
            for resource in find(
                start_resource=self.context.repo[uri],
                matcher=self.match,
                traverse=traverse,
                properties=self.properties,
            ):
                self.resource_count += 1
                print(resource.url)

        logger.info(f'Found {self.resource_count} resource(s)')


def find(
        start_resource: RepositoryResource,
        matcher: Callable[[Iterable], bool],
        traverse: list[URIRef] = None,
        properties: list[tuple] = None
) -> Iterator[RepositoryResource]:
    if traverse is None:
        traverse = []
    if properties is None:
        properties = []
    for resource in start_resource.walk(traverse=traverse):
        if len(properties) > 0:
            subject = URIRef(resource.url)
            if matcher((subject, p, o) in resource.graph for p, o in properties):
                yield resource
        else:
            # with no filters specified, list all resources found
            # this mimics the behavior of the Linux "find" command
            yield resource
