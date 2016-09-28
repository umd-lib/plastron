import hashlib
from io import BytesIO
import mimetypes
import os
import pprint
import requests
import rdflib
from rdflib import Namespace
import sys

#============================================================================
# NAMESPACE BINDINGS
#============================================================================

namespace_manager = rdflib.namespace.NamespaceManager(rdflib.Graph())

bibo = Namespace('http://purl.org/ontology/bibo/')
namespace_manager.bind('bibo', bibo, override=False)

dc = Namespace('http://purl.org/dc/elements/1.1/')
namespace_manager.bind('dc', dc, override=False)

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
        self.auth = None
        self.client_cert = None

        if 'CLIENT_CERT' in config and 'CLIENT_KEY' in config:
            self.client_cert = (config['CLIENT_CERT'], config['CLIENT_KEY'])
        elif 'FEDORA_USER' in config and 'FEDORA_PASSWORD' in config:
            self.auth = (config['FEDORA_USER'], config['FEDORA_PASSWORD'])

        if 'SERVER_CERT' in config:
            self.server_cert = config['SERVER_CERT']
        else:
            self.server_cert = None

    def is_reachable(self):
        response = self.head(self.endpoint)
        return response.status_code == 200

    def post(self, url, **kwargs):
        return requests.post(url, cert=self.client_cert, auth=self.auth,
                verify=self.server_cert, **kwargs)

    def patch(self, url, **kwargs):
        return requests.patch(url, cert=self.client_cert, auth=self.auth,
                verify=self.server_cert, **kwargs)

    def head(self, url, **kwargs):
        return requests.head(url, cert=self.client_cert, auth=self.auth,
                verify=self.server_cert, **kwargs)


#============================================================================
# PCDM RESOURCE (COMMON METHODS FOR ALL OBJECTS)
#============================================================================

class Resource():

    def __init__(self, uri=''):
        self.graph = rdflib.Graph()
        self.graph.namespace_manager = namespace_manager
        self.uri = rdflib.URIRef(uri)


    # create repository object by POSTing object graph
    def create_object(self, repository):
        if self.exists_in_repo(repository):
            return False
        else:
            print("Creating {0}...".format(self.title), end='')
            response = repository.post(repository.endpoint)
            if response.status_code == 201:
                print("success.")
                print(response.status_code, response.text)
                self.uri = rdflib.URIRef(response.text)
                return True
            else:
                print("failed!")
                return False


    # update existing repo object with SPARQL update
    def update_object(self, repository):
        print("Patching {0}...".format(str(self.uri)), end='')
        prolog = ''
        #TODO: limit this to just the prefixes that are used in the graph
        for (prefix, uri) in self.graph.namespace_manager.namespaces():
            prolog += "PREFIX {0}: {1}\n".format(prefix, uri.n3())

        triples = [ "<> {0} {1}.".format(
            self.graph.namespace_manager.normalizeUri(p),
            o.n3()) for (s, p, o) in self.graph ]

        query = prolog + "INSERT DATA {{{0}}}".format("\n".join(triples))
        data = query.encode('utf-8')
        headers = {'Content-Type': 'application/sparql-update'}
        response = repository.patch(str(self.uri), data=data, headers=headers)
        if response.status_code == 204:
            print("success.")
        else:
            print("failed!")
            print(response.status_code, response.text)
        return response


    # recursively create an object and components and that don't yet exist
    def recursive_create(self, repository, nobinaries):
        if not self.exists_in_repo(repository):
            self.create_object(repository)
        else:
            print('Object "{0}" exists. Skipping...'.format(self.title))

        if not nobinaries:
            for file in self.files:
                if not file.exists_in_repo(repository):
                    file.create_nonrdf(repository)
                else:
                    print('File "{0}" exists. Skipping...'.format(file.title))

        for component in self.components:
            if not component.exists_in_repo(repository):
                component.recursive_create(repository, nobinaries)
            else:
                print(
                    'Component "{0}" exists. Skipping...'.format(
                        component.title)
                    )

        if hasattr(self, 'collections'):
            for collection in self.collections:
                if not collection.exists_in_repo(repository):
                    collection.create_object(repository)
                else:
                    print(
                        'Collection "{0}" exists. Skipping...'.format(
                            collection.title)
                        )


    # recursively update an object and all its components and files
    def recursive_update(self, repository, nobinaries):
        self.update_object(repository)
        if not nobinaries:
            for file in self.files:
                file.update_object(repository)
        for component in self.components:
            component.recursive_update(repository, nobinaries)
        if hasattr(self, 'collections'):
            for collection in self.collections:
                collection.update_object(repository)


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
            sys.stderr.write(str(self.uri))
            self.graph.add( (self.uri, pcdm.hasFile, file.uri) )
            file.graph.add( (file.uri, pcdm.fileOf, self.uri) )

        for collection in self.collections:
            self.graph.add( (self.uri, pcdm.memberOf, collection.uri) )
            collection.graph.add( (collection.uri, pcdm.hasMember, self.uri) )


    # show the object's graph, serialized as turtle
    def print_graph(self):
        print(self.graph.serialize(format="turtle").decode())


    # show the item graph and tree of related objects
    def print_item_tree(self):
        print(self.title)
        if self.components:
            for n, p in enumerate(self.components):
                print("  Part {0}: {1}".format(n+1, p.title))
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
        self.graph.add( (self.uri, rdf.type, pcdm.Object) )


    # iterate over each component and create ordering proxies
    def create_ordering(self, repository):

        proxies = []
        for component in self.components:
            position = " ".join([self.sequence_attr[0],
                                getattr(component, self.sequence_attr[1])]
                                )
            proxies.append(Proxy(position, self.title))

        for proxy in proxies:
            proxy.create_object(repository)
            proxy.graph.namespace_manager = self.graph.namespace_manager

        for (position, component) in enumerate(self.components):
            proxy = proxies[position]
            proxy.graph.add( (proxy.uri, ore.proxyFor, component.uri) )
            proxy.graph.add( (proxy.uri, ore.proxyIn, self.uri) )

            if position == 0:
                self.graph.add( (self.uri, iana.first, proxy.uri) )
            else:
                prev = proxies[position - 1]
                proxy.graph.add( (proxy.uri, iana.prev, prev.uri) )

            if position == len(self.components) - 1:
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
        self.graph.add( (self.uri, rdf.type, pcdm.Object) )



#============================================================================
# PCDM FILE
#============================================================================

class File(Resource):

    def __init__(self, localpath):
        Resource.__init__(self)
        self.localpath = localpath
        self.checksum = self.sha1()
        self.filename = os.path.basename(self.localpath)
        self.mimetype = mimetypes.guess_type(self.localpath)[0]
        self.graph.add( (self.uri, rdf.type, pcdm.File) )


    # upload a binary resource
    def create_nonrdf(self, repository):
        print("Loading {0}...".format(self.filename))
        with open(self.localpath, 'rb') as binaryfile:
            data = binaryfile.read()
        headers = {'Content-Type': self.mimetype,
                   'Digest': 'sha1={0}'.format(self.checksum),
                   'Content-Disposition':
                        'attachment; filename="{0}"'.format(self.filename)
                    }
        response = repository.post(repository.endpoint,
                                 data=data,
                                 headers=headers
                                 )
        if response.status_code == 201:
            self.uri = rdflib.URIRef(response.text)
            return True
        else:
            return False


    # update existing binary resource metadata
    def update_object(self, repository):
        patch_uri = str(self.uri) + '/fcr:metadata'
        print("Patching {0}...".format(patch_uri), end='')
        query = "INSERT DATA {{{0}}}".format(
            self.graph.serialize(format='nt').decode()
            )
        data = query.encode('utf-8')
        headers = {'Content-Type': 'application/sparql-update'}
        response = repository.patch(patch_uri, data=data, headers=headers)
        if response.status_code == 204:
            print("success.")
        else:
            print("failed!")
            print(response.status_code, response.text)
        return response


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
        self.graph.add( (self.uri, dc.title, rdflib.Literal(self.title)) )



