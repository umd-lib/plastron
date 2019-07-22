from rdflib import URIRef
from plastron import ldp, ore, rdf
from plastron.exceptions import RESTAPIException
from plastron.namespaces import dcterms, dcmitype, fabio, iana, pcdm, ebucore, pcdmuse
from plastron.util import LocalFile
from PIL import Image

# alias the rdflib Namespace
ns = pcdm

#============================================================================
# PCDM RESOURCE (COMMON METHODS FOR ALL OBJECTS)
#============================================================================

@rdf.object_property('components', pcdm.hasMember, multivalue=True)
@rdf.object_property('collections', pcdm.memberOf, multivalue=True)
@rdf.object_property('files', pcdm.hasFile, multivalue=True)
@rdf.object_property('file_of', pcdm.fileOf, multivalue=True)
@rdf.object_property('related', pcdm.hasRelatedObject, multivalue=True)
@rdf.object_property('related_of', pcdm.relatedObjectOf, multivalue=True)
@rdf.data_property('title', dcterms.title)
class Resource(ldp.Resource):
    def ordered_components(self):
        orig_list = [ obj for obj in self.components if obj.ordered ]
        if not orig_list:
            return []
        else:
            sort_key = self.sequence_attr[1]
            def get_key(item):
                return int(getattr(item, sort_key))
            sorted_list = sorted(orig_list, key=get_key)
            return sorted_list

    def unordered_components(self):
        return [ obj for obj in self.components if not obj.ordered ]

    def add_component(self, obj):
        self.components.append(obj)
        obj.collections.append(self)

    def add_file(self, obj):
        self.files.append(obj)
        obj.file_of.append(self)

    def add_collection(self, obj):
        self.collections.append(obj)

    def add_related(self, obj):
        self.related.append(obj)
        obj.related_of.append(self)

    # show the item graph and tree of related objects
    def print_item_tree(self, indent='', label=None):
        if label is not None:
            print(indent + '[' + label + '] ' + str(self))
        else:
            print(indent + str(self))

        ordered = self.ordered_components()
        if ordered:
            print(indent + '  {Ordered Components}')
            for n, p in enumerate(ordered):
                p.print_item_tree(indent='    ' + indent, label=n)

        unordered = self.unordered_components()
        if unordered:
            print(indent + '  {Unordered Components}')
            for p in unordered:
                p.print_item_tree(indent='     ' + indent)

        files = self.files()
        if files:
            print(indent + '  {Files}')
            for f in files:
                print(indent + '    ' + str(f))


#============================================================================
# PCDM ITEM-OBJECT
#============================================================================

@rdf.object_property('first', iana.first)
@rdf.object_property('last', iana.last)
@rdf.rdf_class(pcdm.Object)
class Item(Resource):

    # iterate over each component and create ordering proxies
    def create_ordering(self, repository):
        proxies = []
        ordered_components = self.ordered_components()
        for component in ordered_components:
            position = " ".join([self.sequence_attr[0],
                getattr(component, self.sequence_attr[1])])
            proxy = ore.Proxy(
                    title=f'Proxy for {position} in {self}',
                    proxy_for=component,
                    proxy_in=self
                    )
            proxies.append(proxy)

        for proxy in proxies:
            proxy.create_object(repository)

        for (position, component) in enumerate(ordered_components):
            proxy = proxies[position]

            if position == 0:
                self.first = proxy
            else:
                proxy.prev = proxies[position - 1]

            if position == len(ordered_components) - 1:
                self.last = proxy
            else:
                proxy.next = proxies[position + 1]

            proxy.update_object(repository)

    def create_annotations(self, repository):
        with repository.at_path('annotations'):
            for annotation in self.annotations:
                annotation.recursive_create(repository)

    def update_annotations(self, repository):
        for annotation in self.annotations:
            annotation.recursive_update(repository)

#============================================================================
# PCDM COMPONENT-OBJECT
#============================================================================

@rdf.rdf_class(pcdm.Object)
class Component(Resource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ordered = False

#============================================================================
# PCDM FILE
#============================================================================

@rdf.data_property('mimetype', ebucore.hasMimeType)
@rdf.data_property('filename', ebucore.filename)
@rdf.data_property('width', ebucore.width)
@rdf.data_property('height', ebucore.height)
@rdf.object_property('dcmitype', dcterms.type)
@rdf.rdf_class(pcdm.File)
class File(Resource):
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

#============================================================================
# PCDM COLLECTION OBJECT
#============================================================================

@rdf.data_property('title', dcterms.title)
@rdf.rdf_class(pcdm.Collection)
class Collection(Resource):

    @classmethod
    def from_repository(cls, repo, uri):
        graph = repo.get_graph(uri)
        collection = cls()
        collection.uri = URIRef(uri)

        # mark as created and updated so that the create_object and update_object
        # methods doesn't try try to modify it
        collection.created = True
        collection.updated = True

        # default title is the URI
        collection.title = str(collection.uri)
        for o in graph.objects(subject=collection.uri, predicate=dcterms.title):
            collection.title = str(o)

        return collection

@rdf.data_property('number', fabio.hasSequenceIdentifier)
@rdf.rdf_class(fabio.Page)
class Page(Resource):
    """One page of an item-level resource"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ordered = True

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
