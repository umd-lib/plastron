from PIL import Image

from plastron.namespaces import dcterms, ebucore, fabio, pcdm, pcdmuse, premis
from plastron.rdf import ldp, ore, rdf

# alias the rdflib Namespace
ns = pcdm


@rdf.object_property('members', pcdm.hasMember)
@rdf.object_property('member_of', pcdm.memberOf)
@rdf.object_property('files', pcdm.hasFile)
@rdf.object_property('related', pcdm.hasRelatedObject)
@rdf.object_property('related_of', pcdm.relatedObjectOf)
@rdf.data_property('title', dcterms.title)
@rdf.rdf_class(pcdm.Object)
class Object(ore.Aggregation):
    def add_member(self, obj):
        self.members.append(obj)
        obj.member_of.append(self)

    def add_file(self, obj):
        self.files.append(obj)
        obj.file_of.append(self)

    def add_related(self, obj):
        self.related.append(obj)
        obj.related_of.append(self)

    def gather_files(self, repository):
        for proxy in self.load_proxies(repository):
            page = Object.from_repository(repository, proxy.proxy_for[0])
            for file_uri in page.files:
                file = File.from_repository(repository, file_uri)
                _, graph = repository.get_graph(file_uri)
                file.read(graph)
                yield file

    # recursively create an object and components and that don't yet exist
    def create(self, client, container_path=None, slug=None, headers=None, recursive=True, **kwargs):
        super().create(
            client=client,
            container_path=container_path,
            slug=slug,
            headers=headers,
            recursive=recursive,
            **kwargs
        )
        if recursive:
            client.create_members(self)
            client.create_files(self)
            client.create_related(self)

    def get_new_member(self, rootname, number):
        return Page(title=f'Page {number}', number=number)


@rdf.object_property('file_of', pcdm.fileOf)
@rdf.data_property('mimetype', ebucore.hasMimeType)
@rdf.data_property('filename', ebucore.filename)
@rdf.data_property('size', premis.hasSize)
@rdf.data_property('width', ebucore.width)
@rdf.data_property('height', ebucore.height)
@rdf.object_property('dcmitype', dcterms.type)
@rdf.data_property('title', dcterms.title)
@rdf.rdf_class(pcdm.File)
class File(ldp.NonRdfSource):
    @classmethod
    def from_repository(cls, client, uri, include_server_managed=True):
        obj = super().from_repository(client, uri, include_server_managed)
        obj.source = RepositoryFileSource(uri=uri, client=client)
        return obj

    @classmethod
    def from_source(cls, source=None, **kwargs):
        obj = super().from_source(source=source, **kwargs)
        obj.mimetype = source.mimetype()
        return obj

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # for image files
        # TODO: move these to a subclass or mix-in?
        self.width = None
        self.height = None

    # upload a binary resource
    def create(self, client, container_path=None, slug=None, headers=None, **kwargs):
        if not client.load_binaries:
            self.logger.info(f'Skipping loading for binary {self.source.filename}')
            return True
        elif self.created:
            return False
        elif self.exists_in_repo(client):
            self.created = True
            return False

        self.logger.info(f'Loading {self.source.filename}')

        if headers is None:
            headers = {}
        headers.update({
            'Content-Type': self.source.mimetype(),
            'Digest': self.source.digest(),
            'Content-Disposition': f'attachment; filename="{self.source.filename}"'
        })

        with self.source as stream:
            super().create(client, container_path=container_path, slug=slug, headers=headers, data=stream, **kwargs)
        self.created = True
        return True

    def update(self, client, recursive=True):
        if not client.load_binaries:
            self.logger.info(f'Skipping update for binary {self.source.filename}')
            return True

        # if this is an image file, see if we can get dimensions
        if self.source.mimetype().startswith('image/'):
            if self.width is None or self.height is None:
                # use PIL
                try:
                    with self.source as stream:
                        with Image.open(stream) as img:
                            self.width = img.width
                            self.height = img.height
                except IOError as e:
                    self.logger.warn(f'Cannot read image file: {e}')

        return super().update(client, recursive=recursive)


@rdf.rdf_class(pcdmuse.PreservationMasterFile)
class PreservationMasterFile(File):
    pass


@rdf.rdf_class(pcdmuse.IntermediateFile)
class IntermediateFile(File):
    pass


@rdf.rdf_class(pcdmuse.ServiceFile)
class ServiceFile(File):
    pass


@rdf.rdf_class(pcdmuse.ExtractedText)
class ExtractedText(File):
    pass


@rdf.rdf_class(pcdm.Collection)
class Collection(Object):
    pass


@rdf.data_property('number', fabio.hasSequenceIdentifier)
@rdf.rdf_class(fabio.Page)
class Page(Object):
    """One page of an item-level resource"""
    pass


FILE_CLASS_FOR = {
    '.tif': PreservationMasterFile,
    '.jpg': IntermediateFile,
    '.txt': ExtractedText,
    '.xml': ExtractedText,
}


def get_file_object(path, source=None):
    extension = path[path.rfind('.'):]
    if extension in FILE_CLASS_FOR:
        cls = FILE_CLASS_FOR[extension]
    else:
        cls = File
    if source is None:
        source = LocalFileSource(path)
    f = cls.from_source(source)
    return f
