import os
import requests
import logging
import threading
from rdflib import Graph, URIRef
from plastron.exceptions import RESTAPIException

class Repository:
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
            self.logger.warning("Unable to connect.")
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

    def create(self, url=None, **kwargs):
        if url is not None:
            response = self.put(url, **kwargs)
        else:
            response = self.post(self.uri(), **kwargs)

        if response.status_code == 201:
            return URIRef(self._remove_transaction_uri(response.headers['Location']))
        else:
            raise RESTAPIException(response)

    def recursive_get(self, url, traverse=None, **kwargs):
        if traverse is None:
            traverse = []
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
        super().__init__(name='TransactionKeepAlive')
        self.repository = repository
        self.interval = interval
        self.stopped = threading.Event()

    def run(self):
        while not self.stopped.wait(self.interval):
            self.repository.maintain_transaction()

    def stop(self):
        self.stopped.set()

