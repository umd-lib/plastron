import logging
from argparse import Namespace

from rdflib import Literal

from plastron.cli.commands import BaseCommand
from plastron.files import LocalFileSource
from plastron.models.pcdm import PCDMFile
from plastron.repo import Repository, BinaryResource, RepositoryError

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='replace',
        description='Replace a binary resource in the repository'
    )
    parser.add_argument(
        '--binary-file',
        help='local path to the binary file',
    )
    parser.add_argument(
        '--mime-type',
        help='use this MIME type instead of auto-detecting based on the BINARY_FILE',
    )
    parser.add_argument(
        'location',
        help='URI or repository path of the resource to replace',
        metavar='URI|PATH',
    )
    parser.set_defaults(cmd_name='replace')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        # mimicking a click.Context object to bridge between argparse and click commands
        ctx = Namespace(obj=self.context)
        return replace(ctx, location=args.location, binary_filename=args.binary_file, mime_type=args.mime_type)


def replace(ctx, location: str, binary_filename: str, mime_type: str = None):
    repo: Repository = ctx.obj.repo
    try:
        resource = repo.get_resource(location, BinaryResource).read()
    except RepositoryError as e:
        raise RuntimeError(str(e)) from e

    if not resource.exists:
        raise RuntimeError(f'Resource {resource.url} not found')

    source = LocalFileSource(binary_filename)

    try:
        with repo.transaction():
            resource.update_binary(source, mime_type=mime_type)
            logger.info(f'Updating metadata for {resource.url}')
            file = resource.describe(PCDMFile)
            file.title = Literal(source.filename)
            resource.update()

        print(resource.url)

    except RepositoryError as e:
        raise RuntimeError(str(e)) from e
