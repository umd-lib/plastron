from typing import List, Tuple, Union

from rdflib import URIRef, Literal

import logging
from plastron.commands import BaseCommand
from plastron.namespaces import get_manager, rdf
from plastron.rdf import parse_predicate_list, parse_data_property, parse_object_property, uri_or_curie
from plastron.core.util import ResourceList

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
    def __call__(self, fcrepo, args):
        self.properties: List[Tuple[URIRef, Union[Literal, URIRef]]] = [
            *(parse_data_property(p, o) for p, o in args.data_properties),
            *(parse_object_property(p, o) for p, o in args.object_properties)
        ]

        for rdf_type in args.types:
            self.properties.append((rdf.type, uri_or_curie(rdf_type)))

        if args.match_any:
            self.match = any
        elif args.match_all:
            self.match = all
        else:
            self.match = all

        logger.info(f'Matching {self.match.__name__} of these properties:')
        for p, o in self.properties:
            logger.info(f'  {p.n3(namespace_manager=manager)} {o.n3(namespace_manager=manager)}')

        self.resource_count = 0

        resources = ResourceList(
            client=fcrepo,
            uri_list=args.uris
        )

        resources.process(
            method=self.find,
            traverse=parse_predicate_list(args.recursive),
            use_transaction=False
        )

        logger.info(f'Found {self.resource_count} resource(s)')

    def find(self, resource, graph):
        if len(self.properties) > 0:
            subject = URIRef(resource.uri)
            if self.match((subject, p, o) in graph for p, o in self.properties):
                self.resource_count += 1
                print(resource)
        else:
            # with no filters specified, list all resources found
            # this mimics the behavior of the Linux "find" command
            self.resource_count += 1
            print(resource)
