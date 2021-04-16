from PIL import Image

from plastron.commands import BaseCommand
from plastron.files import RepositoryFileSource
import logging

Image.MAX_IMAGE_PIXELS = None

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='imgsize',
        description='Add width and height to image resources'
    )
    parser.add_argument(
        'uris',
        nargs='*',
        help='URIs of repository objects to get image info'
    )
    parser.set_defaults(cmd_name='imgsize')


class Command(BaseCommand):
    def __call__(self, fcrepo, args):
        for uri in args.uris:
            source = RepositoryFileSource(fcrepo, uri)
            if source.mimetype().startswith('image/'):
                logger.info(f'Reading image data from {uri}')

                image = Image.open(source.open())

                logger.info(f'URI: {uri}, Width: {image.width}, Height: {image.height}')

                # construct SPARQL query to replace image size metadata
                prolog = 'PREFIX ebucore: <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#>'
                delete = 'DELETE { <> ebucore:width ?w ; ebucore:height ?h }'
                statements = f'<> ebucore:width {image.width} ; ebucore:height {image.height}'
                insert = 'INSERT { ' + statements + ' }'
                where = 'WHERE {}'
                sparql = '\n'.join((prolog, delete, insert, where))

                # update the metadata
                headers = {'Content-Type': 'application/sparql-update'}
                response = fcrepo.patch(source.metadata_uri, data=sparql, headers=headers)
                if response.status_code == 204:
                    logger.info(f'Updated image dimensions on {uri}')
                else:
                    logger.warning(f'Unable to update {uri}')

            else:
                logger.warning(f'{uri} is not of type image/*; skipping')
