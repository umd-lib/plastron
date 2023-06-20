import logging
from argparse import FileType, Namespace
from typing import Iterable
from plastron.cli import get_uris

from plastron.cli.commands import BaseCommand
from plastron.handles import HandleBearingResource
from plastron.namespaces import umdaccess
from plastron.rdfmapping.resources import RDFResource
from plastron.repo import RepositoryError, RepositoryResource

logger = logging.getLogger(__name__)


def get_publication_status(obj: RDFResource) -> str:
    if umdaccess.Published in obj.rdf_type.values:
        if umdaccess.Hidden in obj.rdf_type.values:
            return 'PublishedHidden'
        else:
            return 'Published'
    else:
        if umdaccess.Hidden in obj.rdf_type.values:
            return 'UnpublishedHidden'
        else:
            return 'Unpublished'


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
            resource: RepositoryResource = ctx.obj.repo[uri].read()
        except RepositoryError as e:
            logger.error(f'Unable to retrieve {uri}: {e}')
            continue

        obj = resource.describe(HandleBearingResource)
        if obj.handle.is_valid:
            logger.debug(f'Handle in the repository: {obj.handle.value}')

        handle = ctx.obj.handle_client.get_handle(repo_uri=uri)
        if handle is not None:
            logger.debug(f'Handle service returns handle: {handle.hdl_uri}')

        public_url = ctx.obj.get_public_url(uri)
        if handle is None:
            # create a new handle
            logger.debug(f'Minting new handle for {uri}')
            handle = ctx.obj.handle_client.create_handle(
                repo_uri=uri,
                url=public_url,
            )
            if handle is None:
                # if the handle is still not created, something *really* went wrong
                logger.error(f'Unable to find or create handle for {uri}')
                continue

            obj.handle = handle.hdl_uri
        else:
            # check target URL, and update if needed
            if handle.url != public_url:
                logger.warning(f'Current target URL ({handle.url}) does not match the expected URL ({public_url})')
                handle = ctx.obj.handle_client.update_handle(handle, url=public_url)

            # check to ensure that the handle matches
            if obj.handle.is_valid:
                if handle.hdl_uri != str(obj.handle.value):
                    logger.warning('Handle values differ; updating the repository to match the handle service')
                    obj.handle = handle.hdl_uri

        # add the Published (and optionally, add or remove the Hidden) access classes
        obj.rdf_type.add(umdaccess.Published)
        if force_hidden:
            obj.rdf_type.add(umdaccess.Hidden)
        elif force_visible:
            obj.rdf_type.remove(umdaccess.Hidden)

        # save changes
        resource.update()

        logger.info(f'Publication status of {uri} is {get_publication_status(obj)}')
        logger.info(f'Handle for repo URI {uri} is {handle} ({handle.hdl_uri}) with target URL {handle.url}')
