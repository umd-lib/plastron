from datetime import datetime as dt
import hashlib
from io import BytesIO
import mimetypes
import os
import pprint
import requests
import rdflib
from rdflib import Namespace
import sys
import logging
from uuid import uuid4

#============================================================================
# NAMESPACE BINDINGS
#============================================================================

namespace_manager = rdflib.namespace.NamespaceManager(rdflib.Graph())

bibo = Namespace('http://purl.org/ontology/bibo/')
namespace_manager.bind('bibo', bibo, override=False)

dc = Namespace('http://purl.org/dc/elements/1.1/')
namespace_manager.bind('dc', dc, override=False)

dcterms = Namespace('http://purl.org/dc/terms/')
namespace_manager.bind('dcterms', dcterms, override=False)

ex = Namespace('http://www.example.org/terms/')
namespace_manager.bind('ex', ex, override=False)

foaf = Namespace('http://xmlns.com/foaf/0.1/')
namespace_manager.bind('foaf', foaf, override=False)

iana = Namespace('http://www.iana.org/assignments/relation/')
namespace_manager.bind('iana', iana, override=False)

ore = Namespace('http://www.openarchives.org/ore/terms/')
namespace_manager.bind('ore', ore, override=False)

pcdm = Namespace('http://pcdm.org/models#')
namespace_manager.bind('pcdm', pcdm, override=False)

rdf = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
namespace_manager.bind('rdf', rdf, override=False)

oa = Namespace('http://www.w3.org/ns/oa#')
namespace_manager.bind('oa', oa, override=False)

sc = Namespace('http://www.shared-canvas.org/ns/')
namespace_manager.bind('sc', sc, override=False)

ldp = Namespace('http://www.w3.org/ns/ldp#')
namespace_manager.bind('ldp', ldp, override=False)

prov = Namespace('http://www.w3.org/ns/prov#')
namespace_manager.bind('prov', prov, override=False)

#============================================================================
# REPOSITORY (REPRESENTING AN FCREPO INSTANCE)
#============================================================================

class Repository():
    def __init__(self, config):
        self.endpoint = config['REST_ENDPOINT']
        self.relpath = config['RELPATH']
        self._path_stack = [ self.relpath ]
        self.fullpath = '/'.join(
            [p.strip('/') for p in (self.endpoint, self.relpath)]
            )
        self.auth = None
        self.client_cert = None
        self.transaction = None
        self.load_binaries = True
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )

        if 'CLIENT_CERT' in config and 'CLIENT_KEY' in config:
            self.client_cert = (config['CLIENT_CERT'], config['CLIENT_KEY'])
        elif 'FEDORA_USER' in config and 'FEDORA_PASSWORD' in config:
            self.auth = (config['FEDORA_USER'], config['FEDORA_PASSWORD'])

        if 'SERVER_CERT' in config:
            self.server_cert = config['SERVER_CERT']
        else:
            self.server_cert = None

    def at_path(self, relpath):
        self._path_stack.append(self.relpath)
        self.relpath = relpath
        return self

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        self.relpath = self._path_stack.pop()


    def is_reachable(self):
        response = self.head(self.fullpath)
        return response.status_code == 200

    def post(self, url, **kwargs):
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug("POST {0}".format(target_uri))
        response = requests.post(
            target_uri, cert=self.client_cert,
            auth=self.auth, verify=self.server_cert, **kwargs
            )
        self.logger.debug("%s %s", response.status_code, response.reason)
        return response

    def put(self, url, **kwargs):
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug("PUT {0}".format(target_uri))
        response = requests.put(
            target_uri, cert=self.client_cert,
            auth=self.auth, verify=self.server_cert, **kwargs
            )
        self.logger.debug("%s %s", response.status_code, response.reason)
        return response

    def patch(self, url, **kwargs):
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug("PATCH {0}".format(target_uri))
        response = requests.patch(
            target_uri, cert=self.client_cert,
            auth=self.auth, verify=self.server_cert, **kwargs
            )
        self.logger.debug("%s %s", response.status_code, response.reason)
        return response

    def head(self, url, **kwargs):
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug('HEAD {0}'.format(target_uri))
        response = requests.head(
            target_uri, cert=self.client_cert,
            auth=self.auth, verify=self.server_cert, **kwargs
            )
        self.logger.debug("%s %s", response.status_code, response.reason)
        return response

    def get(self, url, **kwargs):
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug('GET {0}'.format(target_uri))
        response = requests.get(
            target_uri, cert=self.client_cert,
            auth=self.auth, verify=self.server_cert, **kwargs
            )
        self.logger.debug("%s %s", response.status_code, response.reason)
        return response

    def delete(self, url, **kwargs):
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug('DELETE {0}'.format(target_uri))
        response = requests.delete(
            target_uri, cert=self.client_cert,
            auth=self.auth, verify=self.server_cert, **kwargs
            )
        self.logger.debug("%s %s", response.status_code, response.reason)
        return response

    def recursive_get(self, url, traverse=[], **kwargs):
        head_response = self.head(url, **kwargs)
        if 'describedby' in head_response.links:
            target = head_response.links['describedby']['url']
        else:
            target = url
        response = self.get(target, headers={'Accept': 'text/turtle'}, **kwargs)
        if response.status_code == 200:
            graph = rdflib.Graph()
            graph.parse(data=response.text, format='n3', publicID=url)
            yield (self._remove_transaction_uri(url), graph)
            for (s, p, o) in graph:
                if p in traverse:
                    for (uri, graph) in self.recursive_get(
                        str(o), traverse=traverse, **kwargs):
                        yield (uri, graph)

    def get_graph(self, url):
        response = self.get(url, headers={'Accept': 'text/turtle'}, stream=True)
        if response.status_code != 200:
            self.logger.error("Unable to get text/turtle representation of {0}".format(url))
            raise RESTAPIException(response)
        graph = rdflib.Graph()
        graph.parse(source=response.raw, format='turtle')
        return graph

    def open_transaction(self, **kwargs):
        url = os.path.join(self.endpoint, 'fcr:tx')
        self.logger.info("Creating transaction")
        self.logger.debug("POST {0}".format(url))
        response = requests.post(url, cert=self.client_cert, auth=self.auth,
                    verify=self.server_cert, **kwargs)
        self.logger.debug("%s %s", response.status_code, response.reason)
        if response.status_code == 201:
            self.transaction = response.headers['Location']
            self.logger.info("Created transaction {0}".format(self.transaction))
            return True
        else:
            self.logger.error("Failed to create transaction")
            raise RESTAPIException(response)

    def commit_transaction(self, **kwargs):
        if self.transaction is not None:
            url = os.path.join(self.transaction, 'fcr:tx/fcr:commit')
            self.logger.info(
                "Committing transaction {0}".format(self.transaction)
                )
            self.logger.debug("POST {0}".format(url))
            response = requests.post(url, cert=self.client_cert, auth=self.auth,
                        verify=self.server_cert, **kwargs)
            self.logger.debug("%s %s", response.status_code, response.reason)
            if response.status_code == 204:
                self.logger.info(
                    "Committed transaction {0}".format(self.transaction)
                    )
                self.transaction = None
                return True
            else:
                self.logger.error(
                    "Failed to commit transaction {0}".format(self.transaction)
                    )
                raise RESTAPIException(response)

    def rollback_transaction(self, **kwargs):
        if self.transaction is not None:
            url = os.path.join(self.transaction, 'fcr:tx/fcr:rollback')
            response = requests.post(url, cert=self.client_cert, auth=self.auth,
                        verify=self.server_cert, **kwargs)
            if response.status_code == 204:
                self.transaction = None
                return True
            else:
                raise RESTAPIException(response)

    def _insert_transaction_uri(self, uri):
        if self.transaction is None or uri.startswith(self.transaction):
            return uri
        elif uri.startswith(self.endpoint):
            relpath = uri[len(self.endpoint):]
            return '/'.join([p.strip('/') for p in (self.transaction, relpath)])
        else:
            return uri

    def _remove_transaction_uri(self, uri):
        if self.transaction is not None and uri.startswith(self.transaction):
            relpath = uri[len(self.transaction):]
            return '/'.join([p.strip('/') for p in (self.endpoint, relpath)])
        else:
            return uri

#============================================================================
# PCDM RESOURCE (COMMON METHODS FOR ALL OBJECTS)
#============================================================================

class RESTAPIException(Exception):
    def __init__(self, response):
        self.response = response
    def __str__(self):
        return '{0} {1}'.format(self.response.status_code, self.response.reason)

class Resource(object):

    def __init__(self, uri=''):
        self.uri = rdflib.URIRef(uri)
        self.linked_objects = []
        self.fragments = []
        self.annotations = None
        self.extra = rdflib.Graph()
        self.created = False
        self.updated = False
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )

    def components(self):
        return [ obj for (rel, obj) in self.linked_objects if rel == pcdm.hasMember ]

    def ordered_components(self):
        return [ obj for obj in self.components() if obj.ordered ]

    def unordered_components(self):
        return [ obj for obj in self.components() if not obj.ordered ]

    def files(self):
        return [ obj for (rel, obj) in self.linked_objects if rel == pcdm.hasFile ]

    def collections(self):
        return [ obj for (rel, obj) in self.linked_objects if rel == pcdm.memberOf ]

    def related(self):
        return [ obj for (rel, obj) in self.linked_objects if rel == pcdm.hasRelatedObject ]

    def add_component(self, obj):
        self.linked_objects.append((pcdm.hasMember, obj))
        obj.linked_objects.append((pcdm.memberOf, self))

    def add_file(self, obj):
        self.linked_objects.append((pcdm.hasFile, obj))
        obj.linked_objects.append((pcdm.fileOf, self))

    def add_collection(self, obj):
        self.linked_objects.append((pcdm.memberOf, obj))

    def add_related(self, obj):
        self.linked_objects.append((pcdm.hasRelatedObject, obj))
        obj.linked_objects.append((pcdm.relatedObjectOf, self))

    def graph(self):
        graph = rdflib.Graph()
        graph.namespace_manager = namespace_manager

        for (rel, obj) in self.linked_objects:
            graph.add((self.uri, rel, obj.uri))

        for obj in self.fragments:
            graph = graph + obj.graph()

        graph = graph + self.extra

        return graph

    # create repository object by POST or PUT
    def create_object(self, repository, uri=None):
        if self.created:
            return False
        elif self.exists_in_repo(repository):
            self.created = True
            return False

        self.logger.info("Creating {0}...".format(self.title))
        if uri is not None:
            response = repository.put(uri)
        else:
            response = repository.post(
                '/'.join([p.strip('/') for p in (repository.endpoint,
                                                 repository.relpath)])
                )

        if response.status_code == 201:
            self.created = True
            self.logger.info("Created {0}".format(self.title))
            self.uri = rdflib.URIRef(
                repository._remove_transaction_uri(response.text)
                )
            self.uuid = str(self.uri).rsplit('/', 1)[-1]
            self.logger.info(
                'URI: {0} / UUID: {1}'.format(self.uri, self.uuid)
                )
            self.create_fragments()
            return True
        else:
            self.logger.error("Failed to create {0}".format(self.title))
            raise RESTAPIException(response)

    def create_fragments(self):
        for obj in self.fragments:
            obj.uuid = uuid4()
            obj.uri = rdflib.URIRef('{0}#{1}'.format(self.uri, obj.uuid))
            obj.created = True

    # update existing repo object with SPARQL update
    def update_object(self, repository, patch_uri=None):
        graph = self.graph()
        if not patch_uri:
            patch_uri = self.uri
        prolog = ''
        #TODO: limit this to just the prefixes that are used in the graph
        for (prefix, uri) in graph.namespace_manager.namespaces():
            prolog += "PREFIX {0}: {1}\n".format(prefix, uri.n3())

        triples = []
        for (s, p, o) in graph:
            subject = s.n3(graph.namespace_manager)
            if '#' in subject:
                subject = '<' + subject[subject.index('#'):]
            else:
                subject = '<>'
            triples.append("{0} {1} {2}.".format(
                subject,
                graph.namespace_manager.normalizeUri(p),
                o.n3(graph.namespace_manager)
                ))

        query = prolog + "INSERT DATA {{{0}}}".format("\n".join(triples))
        data = query.encode('utf-8')
        headers = {'Content-Type': 'application/sparql-update'}
        self.logger.info("Updating {0}".format(self.title))
        response = repository.patch(str(patch_uri), data=data, headers=headers)
        if response.status_code == 204:
            self.logger.info("Updated {0}".format(self.title))
            self.updated = True
            return response
        else:
            self.logger.error("Failed to update {0}".format(self.title))
            self.logger.error(query)
            raise RESTAPIException(response)

    # recursively create an object and components and that don't yet exist
    def recursive_create(self, repository):
        if self.create_object(repository):
            self.creation_timestamp = dt.now()
        else:
            self.logger.debug('Object "{0}" exists. Skipping.'.format(self.title))

        for (rel, obj) in self.linked_objects:
            if obj.created or obj.exists_in_repo(repository):
                obj.created = True
                self.logger.debug('Object "{0}" exists. Skipping.'.format(self.title))
            else:
                obj.recursive_create(repository)

    # recursively update an object and all its components and files
    def recursive_update(self, repository):
        if not self.updated:
            self.update_object(repository)
            for (rel, obj) in self.linked_objects:
                obj.recursive_update(repository)

    # check for the existence of a local object in the repository
    def exists_in_repo(self, repository):
        if str(self.uri).startswith(repository.endpoint):
            response = repository.head(str(self.uri))
            if response.status_code == 200:
                return True
            else:
                return False
        else:
            return False

    # add arbitrary additional triples provided in a file
    def add_extra_properties(self, triples_file, rdf_format):
        self.extra.parse(
            source=triples_file, format=rdf_format, publicID=self.uri
            )

    # show the object's graph, serialized as turtle
    def print_graph(self):
        print(self.graph().serialize(format="turtle").decode())

    # called after creation of object in repo
    def post_creation_hook(self):
        pass

    # show the item graph and tree of related objects
    def print_item_tree(self):
        print(self.title)
        ordered = self.ordered_components()
        unordered = self.unordered_components()
        if ordered:
            print(" ORDERED COMPONENTS")
            for n, p in enumerate(ordered):
                print("  Part {0}: {1}".format(n+1, p.title))
                for f in p.files:
                    print("   |--{0}: {1}".format(f.title, f.localpath))
        if unordered:
            print(" UNORDERED COMPONENTS")
            for n, p in enumerate(unordered):
                print("  - {1}".format(n+1, p.title))
                for f in p.files:
                    print("   |--{0}: {1}".format(f.title, f.localpath))


#============================================================================
# PCDM ITEM-OBJECT
#============================================================================

class Item(Resource):

    def __init__(self):
        super(Item, self).__init__()
        self.first = None
        self.last = None
        self.annotations = []

    def graph(self):
        graph = super(Item, self).graph()
        graph.add((self.uri, rdf.type, pcdm.Object))
        if self.first is not None:
            graph.add((self.uri, iana.first, self.first.uri))
        if self.last is not None:
            graph.add((self.uri, iana.last, self.last.uri))
        return graph

    # iterate over each component and create ordering proxies
    def create_ordering(self, repository):
        proxies = []
        ordered_components = self.ordered_components()
        for component in ordered_components:
            position = " ".join([self.sequence_attr[0],
                                getattr(component, self.sequence_attr[1])]
                                )
            proxies.append(Proxy(position, proxy_for=component, proxy_in=self))

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

class Component(Resource):

    def __init__(self):
        super(Component, self).__init__()
        self.ordered = False

    def graph(self):
        graph = super(Component, self).graph()
        graph.add((self.uri, rdf.type, pcdm.Object))
        return graph

#============================================================================
# PCDM FILE
#============================================================================

class File(Resource):
    @classmethod
    def from_localpath(cls, localpath, mimetype=None, title=None):
        if mimetype is None:
            mimetype = mimetypes.guess_type(localpath)[0]

        def open_stream():
            return open(localpath, 'rb')

        return cls(
            filename=os.path.basename(localpath),
            mimetype=mimetype,
            title=title,
            open_stream=open_stream
            )

    def __init__(self, filename, mimetype, title=None, open_stream=None):
        super(File, self).__init__()
        self.filename = filename
        self.mimetype = mimetype
        self.open_stream = open_stream
        if title is not None:
            self.title = title
        else:
            self.title = self.filename

    def graph(self):
        graph = super(File, self).graph()
        graph.add((self.uri, rdf.type, pcdm.File))
        graph.add((self.uri, dcterms.title, rdflib.Literal(self.title)))
        return graph

    # upload a binary resource
    def create_object(self, repository):
        if not repository.load_binaries:
            self.logger.info('Skipping loading for binary {0}'.format(self.filename))
            return True
        elif self.created:
            return False
        elif self.exists_in_repo(repository):
            self.created = True
            return False

        checksum = self.sha1()
        self.logger.info("Loading {0}".format(self.filename))
        with self.open_stream() as stream:
            data = stream.read()
        headers = {'Content-Type': self.mimetype,
                   'Digest': 'sha1={0}'.format(checksum),
                   'Content-Disposition':
                        'attachment; filename="{0}"'.format(self.filename)
                    }
        target_uri = '/'.join(
            [p.strip('/') for p in (repository.endpoint, repository.relpath)]
            )
        response = repository.post(target_uri, data=data, headers=headers)
        if response.status_code == 201:
            self.uri = rdflib.URIRef(response.text)
            self.created = True
            return True
        else:
            raise RESTAPIException(response)

    def update_object(self, repository):
        if not repository.load_binaries:
            self.logger.info('Skipping update for binary {0}'.format(self.filename))
            return True
        fcr_metadata = str(self.uri) + '/fcr:metadata'
        super(File, self).update_object(repository, patch_uri=fcr_metadata)

    # generate SHA1 checksum on a file
    def sha1(self):
        BUF_SIZE = 65536
        sha1 = hashlib.sha1()
        with self.open_stream() as stream:
            while True:
                data = stream.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()

#============================================================================
# PCDM COLLECTION OBJECT
#============================================================================

class Collection(Resource):

    def __init__(self):
        Resource.__init__(self)
        self.title = None

    def graph(self):
        graph = super(Collection, self).graph()
        graph.add((self.uri, rdf.type, pcdm.Collection))
        if self.title is not None:
            graph.add((self.uri, dcterms.title, rdflib.Literal(self.title)))
        return graph

#============================================================================
# PCDM PROXY OBJECT
#============================================================================

class Proxy(Resource):

    def __init__(self, position, proxy_for, proxy_in):
        Resource.__init__(self)
        self.title = 'Proxy for {0} in {1}'.format(position, proxy_in.title)
        self.prev = None
        self.next = None
        self.proxy_for = proxy_for
        self.proxy_in = proxy_in

    def graph(self):
        graph = super(Proxy, self).graph()
        graph.add((self.uri, rdf.type, ore.Proxy))
        graph.add((self.uri, dcterms.title, rdflib.Literal(self.title)))
        graph.add((self.uri, ore.proxyFor, self.proxy_for.uri))
        graph.add((self.uri, ore.proxyIn, self.proxy_in.uri))
        if self.prev is not None:
            graph.add((self.uri, iana.prev, self.prev.uri))
        if self.next is not None:
            graph.add((self.uri, iana.next, self.next.uri))
        return graph

    # create proxy object by PUTting object graph
    def create_object(self, repository, **kwargs):
        uri='/'.join([p.strip('/') for p in (self.proxy_for.uri, self.proxy_in.uuid)])
        super(Proxy, self).create_object(repository, uri=uri, **kwargs)

# Annotation resources

class Annotation(Resource):
    def __init__(self):
        super(Annotation, self).__init__()
        self.motivation = None

    def add_body(self, body):
        self.linked_objects.append((oa.hasBody, body))
        self.title = body.title
        body.annotation = self

    def add_target(self, target):
        self.linked_objects.append((oa.hasTarget, target))
        target.annotation = self

    def graph(self):
        graph = super(Annotation, self).graph()
        graph.add((self.uri, rdf.type, oa.Annotation))
        if self.motivation is not None:
            graph.add((self.uri, oa.motivatedBy, self.motivation))
        return graph

class TextualBody(Resource):
    def __init__(self, value, content_type):
        super(TextualBody, self).__init__()
        self.value = value
        self.content_type = content_type
        if len(self.value) <= 25:
            self.title = self.value
        else:
            self.title = self.value[:24] + '…'

    def graph(self):
        graph = super(TextualBody, self).graph()
        graph.add((self.uri, rdf.value, rdflib.Literal(self.value)))
        graph.add((self.uri, dcterms['format'], rdflib.Literal(self.content_type)))
        graph.add((self.uri, rdf.type, oa.TextualBody))
        return graph

class SpecificResource(Resource):
    def __init__(self, source):
        super(SpecificResource, self).__init__()
        self.source = source

    def add_selector(self, selector):
        self.title = selector.title
        self.linked_objects.append((oa.hasSelector, selector))
        selector.annotation = self

    def graph(self):
        graph = super(SpecificResource, self).graph()
        graph.add((self.uri, oa.hasSource, self.source.uri))
        graph.add((self.uri, rdf.type, oa.SpecificResource))
        return graph

class FragmentSelector(Resource):
    def __init__(self, value, conforms_to=None):
        super(FragmentSelector, self).__init__()
        self.value = value
        self.conforms_to = conforms_to
        self.title = self.value

    def graph(self):
        graph = super(FragmentSelector, self).graph()
        graph.add((self.uri, rdf.value, rdflib.Literal(self.value)))
        graph.add((self.uri, rdf.type, oa.FragmentSelector))
        if self.conforms_to is not None:
            graph.add((self.uri, dcterms.conformsTo, self.conforms_to))
        return graph

class XPathSelector(Resource):
    def __init__(self, value):
        super(XPathSelector, self).__init__()
        self.value = value
        self.title = self.value

    def graph(self):
        graph = super(XPathSelector, self).graph()
        graph.add((self.uri, rdf.value, rdflib.Literal(self.value)))
        graph.add((self.uri, rdf.type, oa.XPathSelector))
        return graph
