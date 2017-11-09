import hashlib
import mimetypes
import os
import requests
import logging
import threading
from rdflib import Graph, Literal, URIRef
from classes import ldp, ore
from classes.exceptions import RESTAPIException
from namespaces import dcterms, iana, pcdm, rdf

# alias the RDFlib Namespace
ns = pcdm

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

    def request(self, method, url, **kwargs):
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug("%s %s", method, target_uri)
        response = requests.request(
            method, target_uri, cert=self.client_cert,
            auth=self.auth, verify=self.server_cert, **kwargs
            )
        self.logger.debug("%s %s", response.status_code, response.reason)
        return response

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs):
        return self.request('PATCH', url, **kwargs)

    def head(self, url, **kwargs):
        return self.request('HEAD', url, **kwargs)

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request('DELETE', url, **kwargs)

    def exists(self, url, **kwargs):
        response = self.head(url, **kwargs)
        return response.status_code == 200

    def recursive_get(self, url, traverse=[], **kwargs):
        head_response = self.head(url, **kwargs)
        if 'describedby' in head_response.links:
            target = head_response.links['describedby']['url']
        else:
            target = url
        response = self.get(target, headers={'Accept': 'text/turtle'}, **kwargs)
        if response.status_code == 200:
            graph = Graph()
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
        graph = Graph()
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

    def maintain_transaction(self, **kwargs):
        if self.transaction is not None:
            url = os.path.join(self.transaction, 'fcr:tx')
            self.logger.info(
                "Maintaining transaction {0}".format(self.transaction)
                )
            self.logger.debug("POST {0}".format(url))
            response = requests.post(url, cert=self.client_cert, auth=self.auth,
                        verify=self.server_cert, **kwargs)
            self.logger.debug("%s %s", response.status_code, response.reason)
            if response.status_code == 204:
                self.logger.info(
                    "Transaction {0} is active until {1}".format(
                        self.transaction, response.headers['Expires']
                        )
                    )
                return True
            else:
                self.logger.error(
                    "Failed to maintain transaction {0}".format(self.transaction)
                    )
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

    def uri(self):
        return '/'.join([p.strip('/') for p in (self.endpoint, self.relpath)])


# based on https://stackoverflow.com/a/12435256/5124907
class TransactionKeepAlive(threading.Thread):
    def __init__(self, repository, interval):
        super(TransactionKeepAlive, self).__init__(name='TransactionKeepAlive')
        self.repository = repository
        self.interval = interval
        self.stopped = threading.Event()

    def run(self):
        while not self.stopped.wait(self.interval):
            self.repository.maintain_transaction()

    def stop(self):
        self.stopped.set()


#============================================================================
# PCDM RESOURCE (COMMON METHODS FOR ALL OBJECTS)
#============================================================================

class Resource(ldp.Resource):
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
            proxies.append(ore.Proxy(position, proxy_for=component, proxy_in=self))

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
        graph.add((self.uri, dcterms.title, Literal(self.title)))
        return graph

    # upload a binary resource
    def create_object(self, repository, uri=None):
        if not repository.load_binaries:
            self.logger.info('Skipping loading for binary {0}'.format(self.filename))
            return True
        elif self.created:
            return False
        elif self.exists_in_repo(repository):
            self.created = True
            return False

        self.logger.info("Loading {0}".format(self.filename))

        with self.open_stream() as stream:
            headers = {
                'Content-Type': self.mimetype,
                'Digest': 'sha1={0}'.format(self.sha1()),
                'Content-Disposition': 'attachment; filename="{0}"'.format(self.filename)
                }
            if uri is not None:
                response = repository.put(uri, data=stream, headers=headers)
            else:
                response = repository.post(repository.uri(), data=stream, headers=headers)

        if response.status_code == 201:
            self.uri = URIRef(response.text)
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
        super(Collection, self).__init__()
        self.title = None

    def graph(self):
        graph = super(Collection, self).graph()
        graph.add((self.uri, rdf.type, pcdm.Collection))
        if self.title is not None:
            graph.add((self.uri, dcterms.title, Literal(self.title)))
        return graph
