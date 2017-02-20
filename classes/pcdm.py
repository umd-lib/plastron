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

#============================================================================
# REPOSITORY (REPRESENTING AN FCREPO INSTANCE)
#============================================================================

class Repository():
    def __init__(self, config):
        self.endpoint = config['REST_ENDPOINT']
        self.relpath = config['RELPATH']
        self.fullpath = '/'.join(
            [p.strip('/') for p in (self.endpoint, self.relpath)]
            )
        self.auth = None
        self.client_cert = None
        self.transaction = None
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
        self.graph = rdflib.Graph()
        self.graph.namespace_manager = namespace_manager
        self.uri = rdflib.URIRef(uri)
        self.related = []
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )

    # create repository object by POSTing object graph
    def create_object(self, repository):
        if self.exists_in_repo(repository):
            return False
        else:
            self.logger.info("Creating {0}...".format(self.title))
            response = repository.post(
                '/'.join([p.strip('/') for p in (repository.endpoint,
                                                 repository.relpath)])
                )
            if response.status_code == 201:
                self.logger.info("Created {0}".format(self.title))
                self.uri = rdflib.URIRef(
                    repository._remove_transaction_uri(response.text)
                    )
                self.logger.info('URI: {0}'.format(self.uri))
                return True
            else:
                self.logger.error("Failed to create {0}".format(self.title))
                raise RESTAPIException(response)

    # update existing repo object with SPARQL update
    def update_object(self, repository, patch_uri=None):
        if not patch_uri:
            patch_uri = self.uri
        prolog = ''
        #TODO: limit this to just the prefixes that are used in the graph
        for (prefix, uri) in self.graph.namespace_manager.namespaces():
            prolog += "PREFIX {0}: {1}\n".format(prefix, uri.n3())

        triples = [ "<> {0} {1}.".format(
            self.graph.namespace_manager.normalizeUri(p),
            o.n3(self.graph.namespace_manager)
            ) for (s, p, o) in self.graph ]

        query = prolog + "INSERT DATA {{{0}}}".format("\n".join(triples))
        data = query.encode('utf-8')
        headers = {'Content-Type': 'application/sparql-update'}
        self.logger.info("Updating {0}".format(self.title))
        response = repository.patch(str(patch_uri), data=data, headers=headers)
        if response.status_code == 204:
            self.logger.info("Updated {0}".format(self.title))
            return response
        else:
            self.logger.error("Failed to update {0}".format(self.title))
            self.logger.error(query)
            raise RESTAPIException(response)

    # recursively create an object and components and that don't yet exist
    def recursive_create(self, repository, nobinaries):
        if not self.exists_in_repo(repository):
            self.create_object(repository)
            self.creation_timestamp = dt.now()
        else:
            self.logger.info(
                'Object "{0}" exists. Skipping.'.format(self.title)
                )

        if not nobinaries:
            for file in self.files:
                if not file.exists_in_repo(repository):
                    file.create_nonrdf(repository)
                else:
                    self.logger.info(
                        'File "{0}" exists. Skipping.'.format(file.title)
                        )

        for component in self.components:
            if not component.exists_in_repo(repository):
                component.recursive_create(repository, nobinaries)
            else:
                self.logger.info(
                    'Component "{0}" exists. Skipping.'.format(component.title)
                    )

        for related_object in self.related:
            if not related_object.exists_in_repo(repository):
                related_object.recursive_create(repository, nobinaries)
            else:
                self.logger.info(
                    'Related object "{0}" exists. Skipping.'.format(
                        related_object.title
                        )
                    )

    # recursively update an object and all its components and files
    def recursive_update(self, repository, nobinaries):
        self.update_object(repository)
        if not nobinaries:
            for file in self.files:
                file.update_object(repository)
        for component in self.components:
            component.recursive_update(repository, nobinaries)
        for related_object in self.related:
            related_object.recursive_update(repository, nobinaries)

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

    # update object graph with URI
    def update_subject_uri(self):
        for (s, p, o) in self.graph:
            self.graph.delete( (s, p, o) )
            self.graph.add( (self.uri, p, o) )

    # update membership triples (can be used after repository object creation
    # to ensure repository URIs are present)
    def update_relationship_triples(self):
        for component in self.components:
            self.graph.add( (self.uri, pcdm.hasMember, component.uri) )
            component.graph.add( (component.uri, pcdm.memberOf, self.uri) )
            component.update_relationship_triples()

        for file in self.files:
            # sys.stderr.write(str(self.uri))
            self.graph.add( (self.uri, pcdm.hasFile, file.uri) )
            file.graph.add( (file.uri, pcdm.fileOf, self.uri) )

        for collection in self.collections:
            self.graph.add( (self.uri, pcdm.memberOf, collection.uri) )

        for related_object in self.related:
            self.graph.add(
                (self.uri, pcdm.hasRelatedObject, related_object.uri)
                )
            related_object.graph.add(
                (related_object.uri, pcdm.relatedObjectOf, self.uri)
                )

    # add arbitrary additional triples provided in a file
    def add_extra_properties(self, triples_file, rdf_format):
        self.graph.parse(
            source=triples_file, format=rdf_format, publicID=self.uri
            )

    # show the object's graph, serialized as turtle
    def print_graph(self):
        print(self.graph.serialize(format="turtle").decode())

    # called after creation of object in repo
    def post_creation_hook(self):
        pass

    # show the item graph and tree of related objects
    def print_item_tree(self):
        print(self.title)
        ordered = [c for c in self.components if c.ordered is True]
        unordered = [c for c in self.components if c.ordered is False]
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
        Resource.__init__(self)
        self.files = []
        self.components = []
        self.collections = []
        self.related = []
        self.graph.add( (self.uri, rdf.type, pcdm.Object) )

    # iterate over each component and create ordering proxies
    def create_ordering(self, repository):
        proxies = []
        ordered_components = [c for c in self.components if c.ordered is True]
        for component in ordered_components:
            position = " ".join([self.sequence_attr[0],
                                getattr(component, self.sequence_attr[1])]
                                )
            proxies.append(Proxy(position, self.title))

        for proxy in proxies:
            proxy.create_object(repository)
            proxy.graph.namespace_manager = self.graph.namespace_manager

        for (position, component) in enumerate(ordered_components):
            proxy = proxies[position]
            proxy.graph.add( (proxy.uri, ore.proxyFor, component.uri) )
            proxy.graph.add( (proxy.uri, ore.proxyIn, self.uri) )

            if position == 0:
                self.graph.add( (self.uri, iana.first, proxy.uri) )
            else:
                prev = proxies[position - 1]
                proxy.graph.add( (proxy.uri, iana.prev, prev.uri) )

            if position == len(ordered_components) - 1:
                self.graph.add( (self.uri, iana.last, proxy.uri) )
            else:
                next = proxies[position + 1]
                proxy.graph.add( (proxy.uri, iana.next, next.uri) )

            proxy.update_object(repository)

#============================================================================
# PCDM COMPONENT-OBJECT
#============================================================================

class Component(Resource):

    def __init__(self):
        Resource.__init__(self)
        self.files = []
        self.components = []
        self.collections = []
        self.ordered = False
        self.graph.add( (self.uri, rdf.type, pcdm.Object) )

#============================================================================
# PCDM FILE
#============================================================================

class File(Resource):

    def __init__(self, localpath):
        Resource.__init__(self)
        self.localpath = localpath
        self.graph.add((self.uri, rdf.type, pcdm.File))

    # upload a binary resource
    def create_nonrdf(self, repository):
        checksum = self.sha1()
        mimetype = mimetypes.guess_type(self.localpath)[0]
        self.filename = os.path.basename(self.localpath)
        self.logger.info("Loading {0}".format(self.filename))
        with open(self.localpath, 'rb') as binaryfile:
            data = binaryfile.read()
        headers = {'Content-Type': mimetype,
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
            return True
        else:
            raise RESTAPIException(response)

    def update_object(self, repository):
        fcr_metadata = str(self.uri) + '/fcr:metadata'
        super(File, self).update_object(repository, patch_uri=fcr_metadata)

    # generate SHA1 checksum on a file
    def sha1(self):
        BUF_SIZE = 65536
        sha1 = hashlib.sha1()
        with open(self.localpath, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
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
        self.files = None
        self.graph.add( (self.uri, rdf.type, pcdm.Collection) )

#============================================================================
# PCDM PROXY OBJECT
#============================================================================

class Proxy(Resource):

    def __init__(self, position, context):
        Resource.__init__(self)
        self.title = 'Proxy for {0} in {1}'.format(position, context)
        self.graph.add( (self.uri, rdf.type, ore.Proxy) )
        self.graph.add( (self.uri, dcterms.title, rdflib.Literal(self.title)) )
