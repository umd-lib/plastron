import logging
from os.path import basename
from typing import Optional, Iterator

from rdflib import Literal, URIRef
from urlobject import URLObject

from plastron.client.utils import random_slug
from plastron.files import BinarySource, FileGroup, BinaryResource
from plastron.models.annotations import Annotation
from plastron.models.ldp import LDPContainer
from plastron.models.pcdm import PCDMObject, PCDMFile
from plastron.models.umd import Page
from plastron.repo import ContainerResource, Repository
from plastron.repo.aggregation import AggregationResource

logger = logging.getLogger(__name__)


class WebAnnotationBearingResource(ContainerResource):
    """A container that has an annotations container, containing Web Annotations."""

    def __init__(self, repo: Repository, path: str = None):
        super().__init__(repo, path)
        self.annotations_container = self.get_resource('a', ContainerResource)
        self.annotation_urls: set[URLObject] = set()

    def read(self):
        super().read()
        if self.annotations_container.exists:
            obj = self.annotations_container.describe(LDPContainer)
            for annotation_uri in obj.contains.values:
                self.annotation_urls.add(URLObject(annotation_uri))
        return self

    def create_annotation(self, description: Annotation, slug: str = None) -> ContainerResource:
        if slug is None:
            slug = random_slug()

        if not self.annotations_container.exists:
            logger.debug(f'Creating annotations container for {self.path}')
            self.repo.create(resource_class=ContainerResource, url=self.annotations_container.url)

        annotation_resource = self.annotations_container.create_child(
            resource_class=ContainerResource,
            slug=slug,
            description=description,
        )
        self.annotation_urls.add(annotation_resource.url)
        logger.info(f'Created {annotation_resource.url}')
        return annotation_resource


class PCDMFileBearingResource(ContainerResource):
    """A container that has files, related by the pcdm:hasFile/pcdm:fileOf predicates."""

    def __init__(self, repo: Repository, path: str = None):
        super().__init__(repo, path)
        self.files_container = self.get_resource('f', ContainerResource)
        self.file_urls: set[URLObject] = set()

    def read(self):
        super().read()
        obj = self.describe(PCDMObject)
        for file_uri in obj.has_file.values:
            self.file_urls.add(URLObject(file_uri))
        return self

    def create_file(
            self,
            source: BinarySource,
            slug: Optional[str] = None,
            rdf_types: Optional[set] = None,
    ) -> BinaryResource:
        """Create a single file from the given source as a `pcdm:fileOf` this resource.
        If no slug is provided, one is generated using `random_slug()`."""
        if slug is None:
            slug = random_slug()

        if not self.files_container.exists:
            logger.debug(f'Creating files container for {self.path}')
            self.repo.create(resource_class=ContainerResource, url=self.files_container.url)

        parent = self.describe(PCDMObject)
        title = basename(source.filename)
        logger.info(f'Creating file {source.filename} ({source.mimetype()}) for {parent} as "{title}"')
        # first create the binary with its data
        headers = {
            'Content-Type': source.mimetype() or 'application/octet-stream',
            'Digest': source.digest(),
            'Content-Disposition': f'attachment; filename="{source.filename}"',
        }
        with source.open() as stream:
            file_resource = self.files_container.create_child(
                resource_class=BinaryResource,
                slug=slug,
                data=stream,
                headers=headers,
            )

        # then add its metadata description
        file = file_resource.describe(PCDMFile)
        file.title = title
        file.file_of.add(parent)
        parent.has_file.add(file)
        file.rdf_type.extend(source.rdf_types)
        if rdf_types is not None:
            file.rdf_type.extend(rdf_types)

        file_resource.update()
        self.update()

        self.file_urls.add(file_resource.url)
        logger.debug(f'Created file: {file_resource.url} {title}')
        return file_resource

    def get_files(self, rdf_type: Optional[URIRef] = None, mime_type: Optional[str] = None) -> list[BinaryResource]:
        """Return a list of BinaryResource objects that match either the
        given RDF type or MIME type. If neither is given, includes all files
        for this resource."""
        matched_resources = []
        if rdf_type is not None or mime_type is not None:
            def matches(resource):
                file = resource.describe(PCDMFile)
                return rdf_type in file.rdf_type.values or Literal(mime_type) in file.mime_type.values
        else:
            def matches(_resource):
                return True
        for file_url in self.read().file_urls:
            file_resource = self.repo[file_url:BinaryResource].read()
            if matches(file_resource):
                matched_resources.append(file_resource)
        logger.debug(
            f'Found {len(matched_resources)} file(s) for {self.url} '
            f'with RDF type="{rdf_type or "Any"}" and MIME type="{mime_type or "Any"}"'
        )
        return matched_resources

    def get_file(self, rdf_type: Optional[URIRef] = None, mime_type: Optional[str] = None) -> Optional[BinaryResource]:
        """Return the BinaryResource for the first file of this resource
        matching the given criteria, or None if no such file is found."""
        files = self.read().get_files(rdf_type=rdf_type, mime_type=mime_type)
        try:
            return files[0]
        except IndexError:
            return None


class PCDMObjectResource(PCDMFileBearingResource, AggregationResource):
    """A PCDM Object resource"""
    def __init__(self, repo: Repository, path: str = None):
        super().__init__(repo, path)
        self.members_container = self.get_resource('m', ContainerResource)
        self.member_urls: set[URLObject] = set()

    def read(self):
        super().read()
        for member_uri in self.describe(PCDMObject).has_member.values:
            self.member_urls.add(URLObject(member_uri))
        return self

    def get_members(self) -> list['PCDMObjectResource']:
        return [self.repo[url:PCDMObjectResource] for url in self.member_urls]

    def create_page(self, number: int, file_group: FileGroup, slug: str = None) -> 'PCDMPageResource':
        """Create a page with the given number, as a pcdm:memberOf
        this resource. Files to attach are specified in the file_group.
        If no slug is provided, one is generated using random_slug()."""
        if slug is None:
            slug = random_slug()

        if not self.members_container.exists:
            logger.debug(f'Creating members container for {self.path}')
            self.repo.create(resource_class=ContainerResource, url=self.members_container.url)

        parent = self.describe(PCDMObject)
        logger.info(f'Creating page {number} as "{file_group.label}"')
        page_resource = self.members_container.create_child(
            resource_class=PCDMPageResource,
            slug=slug,
            description=Page(title=Literal(file_group.label), number=Literal(number), member_of=parent),
        )
        parent.has_member.add(URIRef(page_resource.url))
        for file_spec in file_group.files:
            page_resource.create_file(source=file_spec.source, rdf_types=file_spec.rdf_types)
        self.update()
        self.member_urls.add(page_resource.url)
        logger.debug(f'Created page {number}: {page_resource.url} "{file_group.label}"')
        return page_resource

    def create_page_sequence(self, file_groups: dict[str, FileGroup]):
        def create_pages() -> Iterator[PCDMObject]:
            for n, file_group in enumerate(file_groups.values(), 1):
                page_resource = self.create_page(number=n, file_group=file_group)
                yield page_resource.read().describe(Page)

        self.create_sequence(create_pages())


class PCDMPageResource(PCDMFileBearingResource, WebAnnotationBearingResource):
    pass
