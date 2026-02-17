import logging
import re
from argparse import Namespace, FileType
from collections.abc import Iterable

from plastron.cli import get_uris
from plastron.cli.commands import BaseCommand
from plastron.models.ore import Proxy
from plastron.repo import RepositoryResource
from plastron.repo.pcdm import PCDMObjectResource

logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='fixpageorder',
        description='Fix the order of pages in an object, using the page titles as a guide',
    )
    parser.add_argument(
        '-f', '--uris-file',
        action='store',
        type=FileType(),
        help='file containing URIs of objects to fix'
    )
    parser.add_argument(
        '-n', '--dry-run',
        action='store_true',
        help='dry run; do not actually modify the pages',
    )
    parser.add_argument(
        'uris',
        nargs='*',
        help='URIs of objects to fix',
    )
    parser.set_defaults(cmd_name='fixpageorder')


class Command(BaseCommand):
    def __call__(self, args: Namespace):
        # mimicking a click.Context object to bridge between argparse and click commands
        ctx = Namespace(obj=self.context)
        return fix_page_order(ctx, uris=get_uris(args), dry_run=args.dry_run)


def fix_page_order(ctx: Namespace, uris: Iterable[str], dry_run: bool = False):
    page_pattern = re.compile(r'page (\d+)', re.IGNORECASE)
    for uri in uris:
        logger.info(f'Retrieving {uri}...')
        resource = ctx.obj.repo.get_resource(uri, PCDMObjectResource)
        proxy_by_page: dict[int, Proxy] = {}
        proxy_by_uri: dict[str, Proxy] = {}
        resource_by_uri: dict[str, RepositoryResource] = {}
        logger.info('Checking proxies...')
        for proxy in resource.get_proxies():
            resource_by_uri[proxy.url] = proxy
            proxy_obj = proxy.describe(Proxy)
            title = str(proxy_obj.title)
            if m := page_pattern.search(title):
                proxy_by_page[int(m.group(1))] = proxy_obj
                proxy_by_uri[proxy.url] = proxy_obj
            else:
                raise RuntimeError(f'Could not find page number in "{proxy_obj.title}"')

        logger.info('Current order:')
        for page_n, proxy_obj in proxy_by_page.items():
            logger.info(f'{page_n:3d}: {proxy_obj.uri} ({proxy_obj.title})')

        sorted_pages = [proxy_by_page[i] for i in sorted(proxy_by_page.keys())]

        logger.info('Desired order:')
        for page_n, proxy_obj in enumerate(sorted_pages, 1):
            logger.info(f'{page_n:3d}: {proxy_obj.uri} ({proxy_obj.title})')

        if list(proxy_by_page.values()) == sorted_pages:
            logger.info(f'No changes to needed; skipping {uri}')
            continue

        logger.info(f'Modifications required for {uri}')

        if dry_run:
            logger.info(f'Dry run; skipping modifications to the proxies of {uri}')
            continue

        with ctx.obj.repo.transaction():
            for page_n, proxy_obj in enumerate(sorted_pages, 1):
                logger.info(f'Checking whether {proxy_obj.uri} ({proxy_obj.title}) needs an update...')

                prev_proxy = proxy_by_page.get(page_n - 1, None)
                next_proxy = proxy_by_page.get(page_n + 1, None)

                if prev_proxy:
                    proxy_obj.prev = prev_proxy
                else:
                    proxy_obj.prev.clear()

                if next_proxy:
                    proxy_obj.next = next_proxy
                else:
                    proxy_obj.next.clear()

                if proxy_obj.has_changes:
                    logger.info(f'Updating {proxy_obj.uri} ({proxy_obj.title})')
                    resource_by_uri[proxy_obj.uri].update()
                else:
                    logger.info('No changes to {proxy_obj.uri} ({proxy_obj.title})')
