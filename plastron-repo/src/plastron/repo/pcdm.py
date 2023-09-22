import logging
from os.path import basename
from typing import Optional, Set

from rdflib import Literal
from urlobject import URLObject

from plastron.client import random_slug
from plastron.files import BinarySource
from plastron.jobs.utils import FileGroup
from plastron.models import Item, umdform
from plastron.models.umd import PCDMObject, PCDMFile, Page, Proxy
from plastron.rdfmapping.resources import RDFResourceBase
from plastron.repo import ContainerResource, Repository, BinaryResource

logger = logging.getLogger(__name__)


def get_new_member_title(item: RDFResourceBase, rootname: str, number: int) -> Literal:
    if isinstance(item, Item) and umdform.pool_reports in item.format:
        if rootname.startswith('body-'):
            return Literal('Body')
        else:
            return Literal(f'Attachment {number - 1}')
    else:
        return Literal(f'Page {number}')


class WebAnnotationBearingResource(ContainerResource):
    def __init__(self, repo: Repository, path: str = None):
        super().__init__(repo, path)
        self.annotations_container: Optional[ContainerResource] = None
        self.annotation_urls: Set[URLObject] = set()


class PCDMFileBearingResource(ContainerResource):
    """A container that has files, related by the pcdm:hasFile/pcdm:fileOf predicates."""
    def __init__(self, repo: Repository, path: str = None):
        super().__init__(repo, path)
        self.files_container: Optional[ContainerResource] = None
        self.file_urls: Set[URLObject] = set()

    def read(self):
        super().read()
        with self.describe(PCDMObject) as obj:
            for file_uri in obj.has_file.values:
                self.file_urls.add(URLObject(file_uri))

    def create_file(self, source: BinarySource, slug: str = None) -> BinaryResource:
        """Create a single file from the given source as a pcdm:fileOf this resource.
        If no slug is provided, one is generated using random_slug()."""
        if slug is None:
            slug = random_slug()

        if self.files_container is None:
            logger.debug(f'Creating files container for {self.path}')
            self.files_container = self.create_child(resource_class=ContainerResource, slug='f')

        with self.describe(PCDMObject) as parent:
            title = basename(source.filename)
            logger.info(f'Creating file {source.filename} ({source.mimetype()}) for {parent} as "{title}"')
            with source.open() as stream:
                file_resource = self.files_container.create_child(
                    resource_class=BinaryResource,
                    slug=slug,
                    description=PCDMFile(title=title, file_of=parent),
                    data=stream,
                    headers={
                        'Content-Type': source.mimetype(),
                        'Digest': source.digest(),
                        'Content-Disposition': f'attachment; filename="{source.filename}"',
                    },
                )
            with file_resource.describe(PCDMFile) as file:
                parent.has_file.add(file)
        file_resource.save()
        self.file_urls.add(file_resource.url)
        logger.debug(f'Created file: {file_resource.url} {title}')
        return file_resource


class PCDMObjectResource(PCDMFileBearingResource):
    def __init__(self, repo: Repository, path: str = None):
        super().__init__(repo, path)
        self.members_container: Optional[ContainerResource] = None
        self.member_urls: Set[URLObject] = set()
        self.proxies_container: Optional[ContainerResource] = None

    def read(self):
        super().read()
        with self.describe(PCDMObject) as obj:
            for member_uri in obj.has_member.values:
                self.member_urls.add(URLObject(member_uri))

    # TODO: get_sequence() method that walks the proxy sequence and returns an ordered list of resources

    def create_page(self, number: int, file_group: FileGroup, slug: str = None) -> 'PCDMPageResource':
        """Create a page with the given number, as a pcdm:memberOf
        this resource. Files to attach are specified in the file_group.
        If no slug is provided, one is generated using random_slug()."""
        if slug is None:
            slug = random_slug()

        if self.members_container is None:
            logger.debug(f'Creating members container for {self.path}')
            self.members_container = self.create_child(resource_class=ContainerResource, slug='m')

        with self.describe(PCDMObject) as parent:
            title = get_new_member_title(parent, file_group.rootname, number)
            logger.info(f'Creating page {number} for {parent} as "{title}"')
            page_resource = self.members_container.create_child(
                resource_class=PCDMPageResource,
                slug=slug,
                description=Page(title=title, number=number, member_of=parent),
            )
            with page_resource.describe(Page) as page:
                parent.has_member.add(page)
                for file_spec in file_group.files:
                    page_resource.create_file(source=file_spec.source)
        page_resource.save()
        self.member_urls.add(page_resource.url)
        return page_resource

    def create_proxy(self, target: RDFResourceBase, title: str) -> ContainerResource:
        """Create a proxy resource for the given target."""
        if self.proxies_container is None:
            logger.debug(f'Creating proxies container for {self.path}')
            self.proxies_container = self.create_child(resource_class=ContainerResource, slug='x')

        return self.proxies_container.create_child(
            resource_class=ContainerResource,
            description=Proxy(
                proxy_for=target,
                proxy_in=self.describe(PCDMObject),
                title=title,
            ),
            slug=random_slug(),
        )


class PCDMPageResource(PCDMFileBearingResource, WebAnnotationBearingResource):
    pass
