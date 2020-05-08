import logging
import os
import requests
import threading
from collections import namedtuple
from plastron.exceptions import RESTAPIException
from rdflib import Graph, URIRef

OMIT_SERVER_MANAGED_TRIPLES = 'return=representation; omit="http://fedora.info/definitions/v4/repository#ServerManaged"'


# lightweight representation of a resource URI and URI of its description
# for RDFSources, in general the uri and description_uri will be the same
class Resource(namedtuple('Resource', ['uri', 'description_uri'])):
    __slots__ = ()

    def __str__(self):
        return self.uri


class Repository:
    def __init__(self, config, ua_string=None, on_behalf_of=None):
        self.endpoint = config['REST_ENDPOINT']
        self.relpath = config['RELPATH']
        self._path_stack = [self.relpath]
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
        self.delegated_user = on_behalf_of

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
            self.logger.warning("Unable to connect.")
            raise Exception("Unable to connect")

    def request(self, method, url, headers=None, **kwargs):
        if headers is None:
            headers = {}
        target_uri = self._insert_transaction_uri(url)
        self.logger.debug("%s %s", method, target_uri)
        if self.ua_string is not None:
            headers['User-Agent'] = self.ua_string
        if self.delegated_user is not None:
            headers['On-Behalf-Of'] = self.delegated_user
        response = self.session.request(method, target_uri, headers=headers, **kwargs)
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

    def create(self, url=None, **kwargs):
        if url is not None:
            response = self.put(url, **kwargs)
        else:
            response = self.post(self.uri(), **kwargs)

        if response.status_code == 201:
            created_url = response.headers['Location'] if 'Location' in response.headers else url
            return URIRef(self._remove_transaction_uri(created_url))
        else:
            raise RESTAPIException(response)

    def recursive_get(self, url, traverse=None, **kwargs):
        target = self.get_description_uri(url)
        graph = self.get_graph(target)
        resource = Resource(
            uri=self._remove_transaction_uri(url),
            description_uri=self._remove_transaction_uri(target)
        )
        yield resource, graph
        if traverse is not None:
            for (s, p, o) in graph:
                if p in traverse:
                    for (resource, graph) in self.recursive_get(str(o), traverse=traverse, **kwargs):
                        yield resource, graph

    def get_description_uri(self, uri, **kwargs):
        response = self.head(uri, **kwargs)
        if response.status_code != 200:
            raise RESTAPIException(response)
        if 'describedby' in response.links:
            return response.links['describedby']['url']
        else:
            return uri

    def get_graph(self, url, include_server_managed=True):
        description_uri = self.get_description_uri(url)
        headers = {
            'Accept': 'application/n-triples'
        }
        if not include_server_managed:
            headers['Prefer'] = OMIT_SERVER_MANAGED_TRIPLES
        response = self.get(description_uri, headers=headers, stream=True)
        if response.status_code != 200:
            self.logger.error(f"Unable to get {headers['Accept']} representation of {url}")
            raise RESTAPIException(response)
        graph = Graph()
        graph.parse(data=response.text, format='nt')
        return graph

    def build_sparql_update(self, delete_graph, insert_graph):
        # go through each graph and update subjects with transaction IDs
        self._update_subjects_within_transaction(delete_graph)
        self._update_subjects_within_transaction(insert_graph)

        deletes = delete_graph.serialize(format='nt').decode('utf-8').strip()
        inserts = insert_graph.serialize(format='nt').decode('utf-8').strip()
        sparql_update = f"DELETE {{ {deletes} }} INSERT {{ {inserts} }} WHERE {{}}"
        return sparql_update

    def get_transaction_endpoint(self):
        return os.path.join(self.endpoint, 'fcr:tx')

    def in_transaction(self):
        if self.transaction is None:
            return False
        else:
            return self.transaction.active

    def _update_subjects_within_transaction(self, graph):
        if self.in_transaction():
            for s, p, o in graph:
                s_txn = URIRef(self._insert_transaction_uri(str(s)))
                graph.remove((s, p, o))
                graph.add((s_txn, p, o))

    def _insert_transaction_uri(self, uri):
        if not self.in_transaction() or uri.startswith(self.transaction.uri):
            return uri
        elif uri.startswith(self.endpoint):
            relpath = uri[len(self.endpoint):]
            return '/'.join([p.strip('/') for p in (self.transaction.uri, relpath)])
        else:
            return uri

    def _remove_transaction_uri(self, uri):
        if self.in_transaction() and uri.startswith(self.transaction.uri):
            relpath = uri[len(self.transaction.uri):]
            return '/'.join([p.strip('/') for p in (self.endpoint, relpath)])
        else:
            return uri

    def uri(self):
        return '/'.join([p.strip('/') for p in (self.endpoint, self.relpath)])


class Transaction:
    def __init__(self, repository, keep_alive=90):
        self.repository = repository
        self.keep_alive = TransactionKeepAlive(self, keep_alive)
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.active = False
        self.uri = None

    def __enter__(self):
        self.begin()
        return self

    def __str__(self):
        return self.uri

    def __exit__(self, exc_type, exc_val, exc_tb):
        # when we leave the transaction context, always
        # set the stop flag on the keep-alive ping
        self.keep_alive.stop()

    def begin(self):
        self.logger.info('Creating transaction')
        response = self.repository.post(self.repository.get_transaction_endpoint())
        if response.status_code == 201:
            self.uri = response.headers['Location']
            self.repository.transaction = self
            self.logger.info(f'Created transaction {self}')
            self.keep_alive.start()
            self.active = True
        else:
            self.logger.error('Failed to create transaction')
            raise RESTAPIException(response)

    def maintain(self):
        if self.active:
            self.logger.info(f'Maintaining transaction {self}')
            response = self.repository.post(os.path.join(self.uri, 'fcr:tx'))
            if response.status_code == 204:
                self.logger.info(f'Transaction {self} is active until {response.headers["Expires"]}')
            else:
                self.logger.error(f'Failed to maintain transaction {self}')
                raise RESTAPIException(response)

    def commit(self):
        if self.active:
            self.logger.info(f'Committing transaction {self}')
            response = self.repository.post(os.path.join(self.uri, 'fcr:tx/fcr:commit'))
            if response.status_code == 204:
                self.logger.info(f'Committed transaction {self}')
                self.keep_alive.stop()
                self.active = False
            else:
                self.logger.error(f'Failed to commit transaction {self}')
                raise RESTAPIException(response)

    def rollback(self):
        if self.active:
            self.logger.info(f'Rolling back transaction {self}')
            response = self.repository.post(os.path.join(self.uri, 'fcr:tx/fcr:rollback'))
            if response.status_code == 204:
                self.logger.info(f'Rolled back transaction {self}')
                self.keep_alive.stop()
                self.active = False
            else:
                self.logger.error(f'Failed to roll back transaction {self}')
                raise RESTAPIException(response)


# based on https://stackoverflow.com/a/12435256/5124907
class TransactionKeepAlive(threading.Thread):
    def __init__(self, transaction, interval):
        super().__init__(name='TransactionKeepAlive')
        self.transaction = transaction
        self.interval = interval
        self.stopped = threading.Event()

    def run(self):
        while not self.stopped.wait(self.interval):
            self.transaction.maintain()

    def stop(self):
        self.stopped.set()
