from rdflib import URIRef
from plastron import ldp, ore, rdf
from plastron.exceptions import RESTAPIException
from plastron.namespaces import dcterms, dcmitype, ebucore, fabio, pcdm, pcdmuse, premis
from plastron.files import LocalFile, RepositoryFile
from PIL import Image

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
                graph = repository.get_graph(file_uri)
                file.read(graph)
                yield file


@rdf.object_property('file_of', pcdm.fileOf)
@rdf.data_property('mimetype', ebucore.hasMimeType)
@rdf.data_property('filename', ebucore.filename)
@rdf.data_property('size', premis.hasSize)
@rdf.data_property('width', ebucore.width)
@rdf.data_property('height', ebucore.height)
@rdf.object_property('dcmitype', dcterms.type)
@rdf.data_property('title', dcterms.title)
@rdf.rdf_class(pcdm.File)
class File(ldp.Resource):
    @classmethod
    def from_repository(cls, repo, uri, include_server_managed=True):
        source = RepositoryFile(repo, uri)
        return cls(source, uri=uri)

    def __init__(self, source, **kwargs):
        super().__init__(**kwargs)
        self.source = source
        self.filename = source.filename
        if self.title is None:
            self.title = self.filename

    # upload a binary resource
    def create_object(self, repository, uri=None):
        if not repository.load_binaries:
            self.logger.info(f'Skipping loading for binary {self.source.filename}')
            return True
        elif self.created:
            return False
        elif self.exists_in_repo(repository):
            self.created = True
            return False

        self.logger.info(f'Loading {self.source.filename}')

        with self.source.data() as stream:
            headers = {
                'Content-Type': self.source.mimetype(),
                'Digest': self.source.digest(),
                'Content-Disposition': f'attachment; filename="{self.source.filename}"'
            }
            if uri is not None:
                response = repository.put(uri, data=stream, headers=headers)
            else:
                response = repository.post(repository.uri(), data=stream, headers=headers)

        if response.status_code == 201:
            self.uri = URIRef(response.headers['Location'])
            self.created = True
            return True
        else:
            raise RESTAPIException(response)

    def update_object(self, repository, patch_uri=None):
        if not repository.load_binaries:
            self.logger.info(f'Skipping update for binary {self.source.filename}')
            return True

        # if this is an image file, see if we can get dimensions
        if self.source.mimetype().startswith('image/'):
            if self.width is None or self.height is None:
                # use PIL
                try:
                    with Image.open(self.source.data()) as img:
                        self.width = img.width
                        self.height = img.height
                except IOError as e:
                    self.logger.warn(f'Cannot read image file: {e}')

        head_response = repository.head(self.uri)
        if 'describedby' in head_response.links:
            target = head_response.links['describedby']['url']
        else:
            raise Exception(f'Missing describedby Link header for {self.uri}')

        return super().update_object(repository, patch_uri=target)


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


def get_file_object(path):
    extension = path[path.rfind('.'):]
    if extension in FILE_CLASS_FOR:
        cls = FILE_CLASS_FOR[extension]
    else:
        cls = File
    f = cls(LocalFile(path))
    f.dcmitype = dcmitype.Text
    return f
