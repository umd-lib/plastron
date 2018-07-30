import os
import requests
import logging
import threading
from rdflib import Graph, Literal, URIRef
from plastron import ldp, ore
from plastron.exceptions import RESTAPIException
from plastron.namespaces import dcterms, iana, pcdm, rdf
from operator import attrgetter

# alias the RDFlib Namespace
ns = pcdm

#============================================================================
# REPOSITORY (REPRESENTING AN FCREPO INSTANCE)
#============================================================================

class Repository():
    def __init__(self, config, ua_string=None):
        self.endpoint = config['REST_ENDPOINT']
        self.relpath = config['RELPATH']
        self._path_stack = [ self.relpath ]
        self.fullpath = '/'.join(
            [p.strip('/') for p in (self.endpoint, self.relpath)]
            )
        self.session = requests.Session()
        self.transaction = None
        self.load_binaries = True
        self.log_dir = config['LOG_DIR']
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )
        self.ua_string = ua_string

        if 'CLIENT_CERT' in config and 'CLIENT_KEY' in config:
            self.session.cert = (config['CLIENT_CERT'], config['CLIENT_KEY'])
        elif 'FEDORA_USER' in config and 'FEDORA_PASSWORD' in config:
            self.session.auth = (config['FEDORA_USER'], config['FEDORA_PASSWORD'])

        if 'SERVER_CERT' in config:
            self.session.verify = config['SERVER_CERT']

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

    def test_connection(self):
        # test connection to fcrepo
        self.logger.debug("fcrepo.endpoint = %s", self.endpoint)
        self.logger.debug("fcrepo.relpath = %s", self.relpath)
        self.logger.debug("fcrepo.fullpath = %s", self.fullpath)
        self.logger.info("Testing connection to {0}".format(self.fullpath))
        if self.is_reachable():
            self.logger.info("Connection successful.")
        else:
            self.logger.warn("Unable to connect.")
            raise Exception("Unable to connect")

    def request(self, method, url, **kwargs):
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug("%s %s", method, target_uri)
        if self.ua_string is not None:
            if 'headers' not in kwargs:
                kwargs['headers'] = {}
            kwargs['headers']['User-Agent'] = self.ua_string
        response = self.session.request(method, target_uri, **kwargs)
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
        response = self.post(url, **kwargs)
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
            response = self.post(url, **kwargs)
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
            response = self.post(url, **kwargs)
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
            self.logger.info(
                "Rolling back transaction {0}".format(self.transaction)
                )
            response = self.post(url, **kwargs)
            if response.status_code == 204:
                self.logger.info(
                    "Rolled back transaction {0}".format(self.transaction)
                    )
                self.transaction = None
                return True
            else:
                self.logger.error(
                    "Failed to roll back transaction {0}".format(self.transaction)
                    )
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
    def __str__(self):
        if hasattr(self, 'title'):
            return self.title
        else:
            return repr(self)

    def components(self):
        return [ obj for (rel, obj) in self.linked_objects if rel == pcdm.hasMember ]

    def ordered_components(self):
        orig_list = [ obj for obj in self.components() if obj.ordered ]
        if not orig_list:
            return []
        else:
            sort_key = self.sequence_attr[1]
            sorted_list = sorted(orig_list, key=attrgetter(sort_key))
            return sorted_list

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
    def __init__(self, source, title=None):
        super(File, self).__init__()
        self.source = source
        self.filename = source.filename
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
            self.uri = URIRef(response.text)
            self.created = True
            return True
        else:
            raise RESTAPIException(response)

    def update_object(self, repository):
        if not repository.load_binaries:
            self.logger.info(f'Skipping update for binary {self.source.filename}')
            return True
        fcr_metadata = str(self.uri) + '/fcr:metadata'
        super(File, self).update_object(repository, patch_uri=fcr_metadata)


#============================================================================
# PCDM COLLECTION OBJECT
#============================================================================

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

    def __init__(self):
        super(Collection, self).__init__()
        self.title = None

    def graph(self):
        graph = super(Collection, self).graph()
        graph.add((self.uri, rdf.type, pcdm.Collection))
        if self.title is not None:
            graph.add((self.uri, dcterms.title, Literal(self.title)))
        return graph
