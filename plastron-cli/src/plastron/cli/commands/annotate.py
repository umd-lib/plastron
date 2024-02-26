import logging
from argparse import Namespace
from bs4 import BeautifulSoup
from rdflib import URIRef

from plastron.cli import get_uris
from plastron.repo.utils import context
from plastron.cli.commands import BaseCommand
from plastron.models.annotations import FullTextAnnotation, TextualBody
from plastron.namespaces import sc
from plastron.rdfmapping.embed import embedded
from plastron.repo.pcdm import PCDMPageResource


logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='annotate',
        description='Annotate resources with the text content of their HTML files'
    )
    parser.add_argument(
        'uris', nargs='*',
        help='URIs of repository objects to process'
    )
    parser.set_defaults(cmd_name='annotate')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        uris = get_uris(args)
        self.context.client.test_connection()
        for uri in uris:
            with context(repo=self.context.repo):
                obj = self.context.repo[uri:PCDMPageResource]
                for file_resource in obj.read().get_files(mime_type='text/html'):
                    with file_resource.open() as stream:
                        text = BeautifulSoup(stream, features='lxml').get_text()

                    annotation = FullTextAnnotation(
                        motivation=sc.painting,
                        derived_from=URIRef(file_resource.url),
                        body=embedded(TextualBody)(
                            value=text,
                            content_type='text/plain'
                        ),
                        target=URIRef(obj.url)
                    )

                    obj.create_annotation(description=annotation)
