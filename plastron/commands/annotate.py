import logging

from bs4 import BeautifulSoup
from io import BytesIO
from plastron import rdf
from plastron.commands import BaseCommand
from plastron.namespaces import prov, sc
from plastron.oa import Annotation, SpecificResource, TextualBody
from plastron.pcdm import File, Object


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


@rdf.object_property('derived_from', prov.wasDerivedFrom)
class FullTextAnnotation(Annotation):
    pass


class Command(BaseCommand):
    def __init__(self, config=None):
        super().__init__(config)
        self.result = None

    def __call__(self, *args, **kwargs):
        for _ in self.execute(*args, **kwargs):
            pass

    def execute(self, repo, args):
        for target_uri in args.uris:
            obj = Object.from_repository(repo, target_uri)
            for file_uri in obj.files:
                file = File.from_repository(repo, file_uri)

                if str(file.mimetype) == 'text/html':
                    # get text from HTML
                    with file.source as stream:
                        text = BeautifulSoup(BytesIO(b''.join(stream)), features='lxml').get_text()
                else:
                    logger.debug(f'Skipping file with MIME type {file.mimetype}')
                    continue

                annotation = FullTextAnnotation(
                    motivation=sc.painting,
                    derived_from=file
                )

                # don't embed full resources
                if not isinstance(obj, SpecificResource):
                    annotation.props['target'].is_embedded = False

                annotation.add_target(target=obj)
                annotation.add_body(TextualBody(value=text, content_type='text/plain'))

                obj.annotations.append(annotation)
                repo.create_annotations(obj)
                obj.update_annotations(repo)

            yield target_uri
