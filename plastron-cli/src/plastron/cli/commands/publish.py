import logging
from argparse import FileType, Namespace
from typing import Iterable
from plastron.cli import get_uris

from plastron.cli.commands import BaseCommand
from plastron.repo import RepositoryError
from plastron.repo.publish import PublishableResource

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='publish',
        description='Publish digital objects',
    )
    hide_or_show = parser.add_mutually_exclusive_group()
    hide_or_show.add_argument(
        '--hidden',
        action='store_true',
        help='set the "Hidden" state on these objects',
    )
    hide_or_show.add_argument(
        '--visible',
        action='store_true',
        help='remove the "Hidden" state from these objects',
    )
    parser.add_argument(
        '-f', '--uris-file',
        action='store',
        type=FileType(),
        help='file containing URIs of objects to publish'
    )
    parser.add_argument(
        'uris',
        nargs='*',
    )
    parser.set_defaults(cmd_name='publish')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        # mimicking a click.Context object to bridge between argparse and click commands
        ctx = Namespace(obj=self.context)
        return publish(ctx, uris=get_uris(args), force_hidden=args.hidden, force_visible=args.visible)


def publish(ctx, uris: Iterable[str], force_hidden: bool = False, force_visible: bool = False):
    for uri in uris:
        # get the resource and check for an existing handle and current publication status
        try:
            resource: PublishableResource = ctx.obj.repo[uri:PublishableResource].read()
        except RepositoryError as e:
            logger.error(f'Unable to retrieve {uri}: {e}')
            continue

        try:
            handle = resource.publish(
                handle_client=ctx.obj.handle_client,
                public_url=ctx.obj.get_public_url(resource),
                force_hidden=force_hidden,
                force_visible=force_visible,
            )
        except RepositoryError as e:
            logger.error(str(e))
            continue

        logger.info(f'Publication status of {uri} is {resource.publication_status}')
        logger.info(f'Handle for repo URI {uri} is {handle} ({handle.hdl_uri}) with target URL {handle.url}')
