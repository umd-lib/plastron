import logging
from argparse import Namespace

from plastron.cli.commands import BaseCommand
from plastron.files import LocalFileSource, USAGE_TAGS
from plastron.repo import Repository, RepositoryError
from plastron.repo.pcdm import PCDMFileBearingResource

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='attach',
        description='Attach a binary to a resource in the repository'
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
        '--slug',
        help='use this as the resource slug instead of a randomly generated string',
    )
    parser.add_argument(
        '--usage',
        choices=USAGE_TAGS.keys(),
        help='the purpose of this binary',
    )
    parser.add_argument(
        'location',
        help='URI or repository path of the resource to attach the BINARY_FILE to',
        metavar='URI|PATH',
    )
    parser.set_defaults(cmd_name='attach')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        # mimicking a click.Context object to bridge between argparse and click commands
        ctx = Namespace(obj=self.context)
        attach(
            ctx,
            location=args.location,
            binary_filename=args.binary_file,
            slug=args.slug,
            mime_type=args.mime_type,
            usage=args.usage,
        )


def attach(ctx, location: str, binary_filename: str, slug: str = None, mime_type: str = None, usage: str = None):
    repo: Repository = ctx.obj.repo
    try:
        resource = repo.get_resource(location, PCDMFileBearingResource).read()
    except RepositoryError as e:
        raise RuntimeError(str(e)) from e

    if not resource.exists:
        raise RuntimeError(f'Resource {resource.url} not found')

    source = LocalFileSource(binary_filename)

    try:
        with repo.transaction():
            file_resource = resource.create_file(
                source=source,
                slug=slug,
                rdf_types=USAGE_TAGS.get(usage, None),
                mime_type=mime_type,
            )

        print(file_resource.url)

    except RepositoryError as e:
        raise RuntimeError(str(e)) from e
