import logging
from argparse import Namespace

from PIL import Image

from plastron.cli.commands import BaseCommand
from plastron.models.pcdm import PCDMImageFile
from plastron.repo import BinaryResource
from plastron.repo.utils import context

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
    def __call__(self, args: Namespace):
        for uri in args.uris:

            file_resource: BinaryResource = self.context.repo[uri:BinaryResource].read()
            file = file_resource.describe(PCDMImageFile)

            # source = RepositoryFileSource(client, uri)
            if file.mime_type.value.startswith('image/'):  # ?
                with context(repo=self.context.repo):
                    logger.info(f'Reading image data from {uri}')

                    with file_resource.open() as file_contents:
                        image = Image.open(file_contents)

                    logger.info(f'URI: {uri}, Width: {image.width}, Height: {image.height}')
                    file.width = image.width
                    file.height = image.height
                    file_resource.update()

            else:
                logger.warning(f'{uri} is not of type image/*; skipping')
