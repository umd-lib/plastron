import logging
import os
import requests
import threading
import time
from base64 import urlsafe_b64encode
from collections import namedtuple
from jwcrypto.jwk import JWK
from jwcrypto.jwt import JWT
from plastron.exceptions import ConfigError, FailureException, RESTAPIException
from rdflib import Graph, URIRef
from requests.exceptions import ConnectionError
from urllib.parse import urlsplit


OMIT_SERVER_MANAGED_TRIPLES = 'return=representation; omit="http://fedora.info/definitions/v4/repository#ServerManaged"'


def random_slug(length=6):
    return urlsafe_b64encode(os.urandom(length)).decode()


def auth_token(secret: str, valid_for=3600) -> JWT:
    """
    Create an admin auth token from the specified secret. By default, the token
    will be valid for 1 hour (3600 seconds).

    :param secret:
    :param valid_for:
    :return:
    """
    token = JWT(
        header={
            'alg': 'HS256'
        },
        claims={
            'sub': 'plastron',
            'iss': 'plastron',
            'exp': time.time() + valid_for,
            'role': 'fedoraAdmin'
        }
    )
    key = JWK(kty='oct', k=secret)
    token.make_signed_token(key)
    return token


# lightweight representation of a resource URI and URI of its description
# for RDFSources, in general the uri and description_uri will be the same
class ResourceURI(namedtuple('Resource', ['uri', 'description_uri'])):
    __slots__ = ()

    def __str__(self):
        return self.uri


class FlatCreator:
    """
    Creates all linked objects at the same container level as the initial item.
    However, proxies and annotations are still created within child containers
    of the item.
    """
    def __init__(self, repository):
        self.repository = repository

    def create_members(self, item):
        self.repository.create_all(item.container_path, item.members)

    def create_files(self, item):
        self.repository.create_all(item.container_path, item.files)

    def create_proxies(self, item):
        # create as child resources, grouped by relationship
        self.repository.create_all(item.path + '/x', item.proxies(), name_function=random_slug)

    def create_related(self, item):
        self.repository.create_all(item.container_path, item.related)

    def create_annotations(self, item):
        # create as child resources, grouped by relationship
        self.repository.create_all(item.path + '/a', item.annotations, name_function=random_slug)


class HierarchicalCreator:
    """
    Creates linked objects in an ldp:contains hierarchy, using intermediate
    containers to group by relationship:

    * Members = /m
    * Files = /f
    * Proxies = /x
    * Annotations = /a

    Related items, however, are created in the same container as the initial
    item.
    """
    def __init__(self, repository):
        self.repository = repository

    def create_members(self, item):
        self.repository.create_all(item.path + '/m', item.members, name_function=random_slug)

    def create_files(self, item):
        # create as child resources, grouped by relationship
        self.repository.create_all(item.path + '/f', item.files, name_function=random_slug)

    def create_proxies(self, item):
        # create as child resources, grouped by relationship
        self.repository.create_all(item.path + '/x', item.proxies(), name_function=random_slug)

    def create_related(self, item):
        # create related objects at the same container level as this object
        self.repository.create_all(item.container_path, item.related)

    def create_annotations(self, item):
        # create as child resources, grouped by relationship
        self.repository.create_all(item.path + '/a', item.annotations, name_function=random_slug)


class Repository:
    def __init__(self, config, ua_string=None, on_behalf_of=None):
        # repo root
        self.endpoint = config['REST_ENDPOINT'].rstrip('/')

        # Extract endpoint URL components for managing forward host
        parsed_endpoint_url = urlsplit(self.endpoint)
        endpoint_proto = parsed_endpoint_url.scheme
        endpoint_host = parsed_endpoint_url.hostname
        self.endpoint_base_path = parsed_endpoint_url.path

        # default container path
        self.relpath = config['RELPATH']
        if not self.relpath.startswith('/'):
            self.relpath = '/' + self.relpath
        self._path_stack = [self.relpath]
        self.fullpath = '/'.join(
            [p.strip('/') for p in (self.endpoint, self.relpath)]
        )
        self.session = requests.Session()
        self.jwt_secret = None
        self.transaction = None
        self.load_binaries = True
        self.log_dir = config['LOG_DIR']
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.ua_string = ua_string
        self.delegated_user = on_behalf_of

        repo_external_url = config.get('REPO_EXTERNAL_URL')
        self.forwarded_host = None
        self.forwarded_proto = None
        self.forwarded_endpoint = None
        if repo_external_url is not None:
            parsed_repo_external_url = urlsplit(repo_external_url)
            proto = parsed_repo_external_url.scheme
            host = parsed_repo_external_url.hostname
            if (proto != endpoint_proto) or (host != endpoint_host):
                # We are forwarding
                self.forwarded_host = host
                self.forwarded_proto = proto

                self.session.headers.update(
                    {
                        'X-Forwarded-Host': self.forwarded_host,
                        'X-Forwarded-Proto': self.forwarded_proto
                    }
                )

                self.forwarded_endpoint = f"{self.forwarded_proto}://{self.forwarded_host}{self.endpoint_base_path}"

        structure_type = config.get('STRUCTURE', 'flat').lower()
        if structure_type == 'hierarchical':
            self.creator = HierarchicalCreator(self)
        elif structure_type == 'flat':
            self.creator = FlatCreator(self)
        else:
            raise ConfigError(f'Unknown STRUCTURE value: {structure_type}')

        # set up authentication credentials; in order of preference:
        #   1. Bearer token
        #   2. SSL client cert
        #   3. HTTP Basic username/password
        if 'AUTH_TOKEN' in config:
            self.session.headers.update(
                {'Authorization': f"Bearer {config['AUTH_TOKEN']}"}
            )
        elif 'JWT_SECRET' in config:
            self.session.headers.update(
                {'Authorization': f"Bearer {auth_token(config['JWT_SECRET']).serialize()}"}
            )
        elif 'CLIENT_CERT' in config and 'CLIENT_KEY' in config:
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

    def is_forwarded(self):
        return self.forwarded_endpoint is not None

    def is_reachable(self):
        try:
            response = self.head(self.fullpath)
            return response.status_code == 200
        except requests.exceptions.ConnectionError as e:
            self.logger.error(str(e))
            return False

    def test_connection(self):
        # test connection to fcrepo
        self.logger.debug(f"Endpoint = {self.endpoint}")
        self.logger.debug(f"Default container path = {self.relpath}")
        self.logger.info(f"Testing connection to {self.endpoint + self.relpath}")
        if self.is_reachable():
            self.logger.info("Connection successful.")
        else:
            raise ConnectionError(f'Unable to connect to {self.fullpath}')

    def request(self, method, url, headers=None, **kwargs):
        if headers is None:
            headers = {}
        target_uri = self._insert_transaction_uri(url)

        if self.is_forwarded():
            # Reverse forward
            target_uri = self.undo_forward(target_uri)

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

    def path_exists(self, path, **kwargs):
        return self.exists(self.endpoint + path, **kwargs)

    def create(self, path=None, url=None, container_path=None, **kwargs):
        if url is not None:
            response = self.put(url, **kwargs)
        elif path is not None:
            response = self.put(self.endpoint + path, **kwargs)
        else:
            container_uri = self.endpoint + (container_path or self.relpath)
            response = self.post(container_uri, **kwargs)

        if response.status_code == 201:
            created_uri = self._remove_transaction_uri(
                response.headers['Location'] if 'Location' in response.headers else url
            )
            created_uri = self.handle_forward(created_uri)
            description_uri = self.process_description_uri(created_uri, response)

            return ResourceURI(created_uri, description_uri)
        else:
            raise RESTAPIException(response)

    def create_all(self, container_path, resources, name_function=None):
        # ensure the container exists
        if len(resources) > 0 and not self.path_exists(container_path):
            self.create(path=container_path)

        for obj in resources:
            if obj.created or obj.exists_in_repo(self):
                obj.created = True
                self.logger.debug(f'Object "{obj}" exists. Skipping.')
            else:
                slug = name_function() if callable(name_function) else None
                obj.create(self, container_path=container_path, slug=slug)

    def create_members(self, item):
        self.creator.create_members(item)

    def create_files(self, item):
        self.creator.create_files(item)

    def create_proxies(self, item):
        self.creator.create_proxies(item)

    def create_related(self, item):
        self.creator.create_related(item)

    def create_annotations(self, item):
        self.creator.create_annotations(item)

    def recursive_get(self, url, traverse=None, **kwargs):
        target = self.get_description_uri(url)
        graph = self.get_graph(target)
        resource = ResourceURI(
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
        return self.process_description_uri(uri, response)

    def process_description_uri(self, uri, response):
        result = None

        if 'describedby' in response.links:
            result = response.links['describedby']['url']
        else:
            result = uri

        result = self.handle_forward(result)
        return result

    def handle_forward(self, url):
        if not self.is_forwarded():
            return url

        # Replace endpoint URL with forwarded host URL
        parsed_url = urlsplit(url)
        if self.forwarded_proto:
            parsed_url = parsed_url._replace(scheme=self.forwarded_proto)
        if self.forwarded_host:
            parsed_url = parsed_url._replace(netloc=self.forwarded_host)

        return parsed_url.geturl()

    def undo_forward(self, url):
        # Replace forwarded host URL with endpoint URL (if needed)
        result = url
        if self.is_forwarded() and url.startswith(self.forwarded_endpoint):
            parsed_url = urlsplit(url)
            full_path = parsed_url.path
            subpath = full_path[len(self.endpoint_base_path):]
            result = self.endpoint + subpath
            if (parsed_url.fragment is not None) and (len(parsed_url.fragment) > 0):
                result = self.endpoint + subpath + "#" + parsed_url.fragment
        return result

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

    def build_sparql_update(self, delete_graph=None, insert_graph=None):
        # go through each graph and update subjects with transaction IDs
        if delete_graph is not None:
            self._update_subjects_within_transaction(delete_graph)
            deletes = delete_graph.serialize(format='nt').decode('utf-8').strip()
        else:
            deletes = None

        if insert_graph is not None:
            self._update_subjects_within_transaction(insert_graph)
            inserts = insert_graph.serialize(format='nt').decode('utf-8').strip()
        else:
            inserts = None

        if deletes is not None and inserts is not None:
            return f"DELETE {{ {deletes} }} INSERT {{ {inserts} }} WHERE {{}}"
        elif deletes is not None:
            return f"DELETE DATA {{ {deletes} }}"
        elif inserts is not None:
            return f"INSERT DATA {{ {inserts} }}"
        else:
            return ''

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

        if self.is_forwarded():
            uri = self.undo_forward(uri)

        if uri.startswith(self.endpoint):
            relpath = uri[len(self.endpoint):]
            uri = '/'.join([p.strip('/') for p in (self.transaction.uri, relpath)])
            return uri
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
        try:
            self.begin()
        except TransactionError as e:
            raise FailureException(f'Transaction failed: {e}')
        return self

    def __str__(self):
        return self.uri

    def __exit__(self, exc_type, exc_val, exc_tb):
        # when we leave the transaction context, always
        # set the stop flag on the keep-alive ping
        self.keep_alive.stop()
        # on an exception, rollback the transaction
        if exc_type is not None:
            if exc_type == TransactionError:
                raise FailureException(f'Transaction failed: {exc_val}')
            self.rollback()
            # return false to propagate the exception upward
            return False

    def begin(self):
        self.logger.info('Creating transaction')
        try:
            response = self.repository.post(self.repository.get_transaction_endpoint())
        except ConnectionError as e:
            raise TransactionError(f'Failed to create transaction: {e}') from e
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
            try:
                response = self.repository.post(os.path.join(self.uri, 'fcr:tx'))
            except ConnectionError as e:
                raise TransactionError(f'Failed to maintain transaction {self}: {e}') from e
            if response.status_code == 204:
                self.logger.info(f'Transaction {self} is active until {response.headers["Expires"]}')
            else:
                self.logger.error(f'Failed to maintain transaction {self}')
                raise RESTAPIException(response)

    def commit(self):
        if self.active:
            self.keep_alive.stop()
            self.active = False
            self.logger.info(f'Committing transaction {self}')
            try:
                response = self.repository.post(os.path.join(self.uri, 'fcr:tx/fcr:commit'))
            except ConnectionError as e:
                raise TransactionError(f'Failed to commit transaction {self}: {e}') from e
            if response.status_code == 204:
                self.logger.info(f'Committed transaction {self}')
            else:
                self.logger.error(f'Failed to commit transaction {self}')
                raise RESTAPIException(response)

    def rollback(self):
        if self.active:
            self.keep_alive.stop()
            self.active = False
            self.logger.info(f'Rolling back transaction {self}')
            try:
                response = self.repository.post(os.path.join(self.uri, 'fcr:tx/fcr:rollback'))
            except ConnectionError as e:
                raise TransactionError(f'Failed to roll back transaction {self}: {e}') from e
            if response.status_code == 204:
                self.logger.info(f'Rolled back transaction {self}')
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


class TransactionError(Exception):
    pass
