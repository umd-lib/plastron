import logging
import os
import threading
from base64 import urlsafe_b64encode
from collections import namedtuple
from contextlib import contextmanager
from enum import Enum
from typing import Mapping, Any, Optional, List, Callable, Tuple

import requests
from rdflib import Graph, URIRef
from requests import PreparedRequest, Response
from requests.auth import AuthBase, HTTPBasicAuth
from requests.exceptions import ConnectionError
from requests_jwtauth import HTTPBearerAuth, JWTSecretAuth
from urlobject import URLObject

logger = logging.getLogger(__name__)

OMIT_SERVER_MANAGED_TRIPLES = 'return=representation; omit="http://fedora.info/definitions/v4/repository#ServerManaged"'


def random_slug(length=6):
    return urlsafe_b64encode(os.urandom(length)).decode()


# lightweight representation of a resource URI and URI of its description
# for RDFSources, in general the uri and description_uri will be the same
class ResourceURI(namedtuple('Resource', ['uri', 'description_uri'])):
    __slots__ = ()

    def __str__(self):
        return self.uri


class RESTAPIException(Exception):
    def __init__(self, response):
        self.response = response

    def __str__(self):
        return f'{self.response.status_code} {self.response.reason}'


class FlatCreator:
    """
    Creates all linked objects at the same container level as the initial item.
    However, proxies and annotations are still created within child containers
    of the item.
    """
    def __init__(self, client: 'Client'):
        self.client = client

    def create_members(self, item):
        self.client.create_all(item.container_path, item.members)

    def create_files(self, item):
        self.client.create_all(item.container_path, item.files)

    def create_proxies(self, item):
        # create as child resources, grouped by relationship
        self.client.create_all(item.path + '/x', item.proxies(), name_function=random_slug)

    def create_related(self, item):
        self.client.create_all(item.container_path, item.related)

    def create_annotations(self, item):
        # create as child resources, grouped by relationship
        self.client.create_all(item.path + '/a', item.annotations, name_function=random_slug)


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
    def __init__(self, client: 'Client'):
        self.client = client

    def create_members(self, item):
        self.client.create_all(item.path + '/m', item.members, name_function=random_slug)

    def create_files(self, item):
        # create as child resources, grouped by relationship
        self.client.create_all(item.path + '/f', item.files, name_function=random_slug)

    def create_proxies(self, item):
        # create as child resources, grouped by relationship
        self.client.create_all(item.path + '/x', item.proxies(), name_function=random_slug)

    def create_related(self, item):
        # create related objects at the same container level as this object
        self.client.create_all(item.container_path, item.related)

    def create_annotations(self, item):
        # create as child resources, grouped by relationship
        self.client.create_all(item.path + '/a', item.annotations, name_function=random_slug)


class ClientCertAuth(AuthBase):
    def __init__(self, cert: str, key: str):
        self.cert = cert
        self.key = key

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.cert = (self.cert, self.key)
        return request


def get_authenticator(config: Mapping[str, Any]) -> Optional[AuthBase]:
    if 'AUTH_TOKEN' in config:
        return HTTPBearerAuth(token=config['AUTH_TOKEN'])
    elif 'JWT_SECRET' in config:
        return JWTSecretAuth(
            secret=config['JWT_SECRET'],
            claims={
                'sub': 'plastron',
                'iss': 'plastron',
                'role': 'fedoraAdmin'
            }
        )
    elif 'CLIENT_CERT' in config and 'CLIENT_KEY' in config:
        return ClientCertAuth(
            cert=config['CLIENT_CERT'],
            key=config['CLIENT_KEY'],
        )
    elif 'FEDORA_USER' in config and 'FEDORA_PASSWORD' in config:
        return HTTPBasicAuth(
            username=config['FEDORA_USER'],
            password=config['FEDORA_PASSWORD'],
        )
    else:
        return None


class Repository:
    def __init__(self, endpoint: str, default_path: str = '/', external_url: str = None):
        self.endpoint = URLObject(endpoint)

        # default container path
        self.relpath = default_path
        if not self.relpath.startswith('/'):
            self.relpath = '/' + self.relpath

        if external_url is not None:
            self.external_url = URLObject(external_url)
        else:
            self.external_url: Optional[URLObject] = None

    def contains(self, uri: str) -> bool:
        """
        Returns True if the given URI string is contained within this
        repository, False otherwise
        """
        return uri.startswith(self.endpoint) or (self.external_url is not None and uri.startswith(self.external_url))

    def repo_path(self, resource_uri: Optional[str]) -> Optional[str]:
        """
        Returns the repository path for the given resource URI, i.e. the
        path with either the "REST_ENDPOINT" or "REPO_EXTERNAL_URL"
        removed.
        """
        if resource_uri is None:
            return None
        elif self.external_url:
            return resource_uri.replace(self.external_url, '')
        else:
            return resource_uri.replace(self.endpoint, '')

    @property
    def transaction_endpoint(self):
        return os.path.join(self.endpoint, 'fcr:tx')

    def uri(self):
        return '/'.join([p.strip('/') for p in (self.endpoint, self.relpath)])


class RepositoryStructure(Enum):
    FLAT = 0
    HIERARCHICAL = 1


class Client:
    def __init__(
            self,
            repo: Repository,
            structure: RepositoryStructure = RepositoryStructure.FLAT,
            auth: AuthBase = None,
            server_cert: str = None,
            ua_string: str = None,
            on_behalf_of: str = None,
            load_binaries: bool = True,
    ):
        self.repo = repo
        self.structure = structure
        self.ua_string = ua_string
        self.delegated_user = on_behalf_of
        self.load_binaries = load_binaries

        self.session = requests.Session()
        self.session.auth = auth
        if server_cert is not None:
            self.session.verify = server_cert

        # set session-wide headers
        if self.ua_string is not None:
            self.session.headers.update({'User-Agent': self.ua_string})
        if self.delegated_user is not None:
            self.session.headers.update({'On-Behalf-Of': self.delegated_user})
        if self.repo.external_url is not None:
            self.session.headers.update({
                'X-Forwarded-Host': self.repo.external_url.hostname,
                'X-Forwarded-Proto': self.repo.external_url.scheme,
            })

        # set creator strategy for the repository
        if self.structure is RepositoryStructure.HIERARCHICAL:
            self.creator = HierarchicalCreator(self)
        elif self.structure is RepositoryStructure.FLAT:
            self.creator = FlatCreator(self)
        else:
            raise RuntimeError(f'Unknown STRUCTURE value: {structure}')

    def request(self, method, url, **kwargs) -> Response:
        logger.debug(f'{method} {url}')
        response = self.session.request(method, url, **kwargs)
        logger.debug(f'{response.status_code} {response.reason}')
        return response

    def post(self, url, **kwargs) -> Response:
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs) -> Response:
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs) -> Response:
        return self.request('PATCH', url, **kwargs)

    def head(self, url, **kwargs) -> Response:
        return self.request('HEAD', url, **kwargs)

    def get(self, url, **kwargs) -> Response:
        return self.request('GET', url, **kwargs)

    def delete(self, url, **kwargs) -> Response:
        return self.request('DELETE', url, **kwargs)

    def get_description(
            self,
            uri: str,
            content_type: str = 'application/n-triples',
            include_server_managed: bool = True
    ) -> Tuple[ResourceURI, str]:
        description_uri = self.get_description_uri(uri)
        headers = {
            'Accept': content_type,
        }
        if not include_server_managed:
            headers['Prefer'] = OMIT_SERVER_MANAGED_TRIPLES
        response = self.get(description_uri, headers=headers, stream=True)
        if not response.ok:
            logger.error(f"Unable to get {headers['Accept']} representation of {uri}")
            raise RESTAPIException(response)
        resource = ResourceURI(uri=uri, description_uri=description_uri)
        text = response.text
        return resource, text

    def get_graph(self, uri: str, include_server_managed: bool = True) -> Tuple[ResourceURI, Graph]:
        resource, text = self.get_description(uri, include_server_managed=include_server_managed)
        graph = Graph()
        graph.parse(data=text, format='nt')
        return resource, graph

    def recursive_get(self, uri: str, traverse: Optional[List[URIRef]] = None):
        resource, graph = self.get_graph(uri)
        yield resource, graph
        if traverse is not None:
            for (s, p, o) in graph:
                if p in traverse:
                    for (resource, graph) in self.recursive_get(str(o), traverse=traverse):
                        yield resource, graph

    def get_description_uri(self, uri: str, response: Response = None) -> str:
        if response:
            if not response.ok:
                raise RESTAPIException(response)
            try:
                return response.links['describedby']['url']
            except KeyError:
                return uri

        # only if we didn't get a response argument do we make a request
        return self.get_description_uri(uri, response=self.head(uri))

    def is_reachable(self):
        try:
            response = self.head(self.repo.endpoint)
            return response.status_code == 200
        except requests.exceptions.ConnectionError as e:
            logger.error(str(e))
            return False

    def test_connection(self):
        # test connection to fcrepo
        logger.debug(f"Endpoint = {self.repo.endpoint}")
        logger.debug(f"Default container path = {self.repo.relpath}")
        logger.info(f"Testing connection to {self.repo.endpoint}")
        if self.is_reachable():
            logger.info("Connection successful.")
        else:
            raise ConnectionError(f'Unable to connect to {self.repo.endpoint}')

    def exists(self, uri: str, **kwargs) -> bool:
        response = self.head(uri, **kwargs)
        return response.status_code == 200

    def path_exists(self, path: str, **kwargs) -> bool:
        return self.exists(self.repo.endpoint + path, **kwargs)

    def get_location(self, response: Response) -> Optional[str]:
        try:
            return response.headers['Location']
        except KeyError:
            logger.warning('No Location header in response')
            return None

    def create(self, path: str = None, url: str = None, container_path: str = None, **kwargs) -> ResourceURI:
        if url is not None:
            response = self.put(url, **kwargs)
        elif path is not None:
            response = self.put(self.repo.endpoint + path, **kwargs)
        else:
            container_uri = self.repo.endpoint + (container_path or self.repo.relpath)
            response = self.post(container_uri, **kwargs)

        if response.status_code == 201:
            created_uri = self.get_location(response) or url
            description_uri = self.get_description_uri(created_uri, response)

            return ResourceURI(created_uri, description_uri)
        else:
            raise RESTAPIException(response)

    def create_all(self, container_path: str, resources: List[Any], name_function: Callable = None):
        # ensure the container exists
        if len(resources) > 0 and not self.path_exists(container_path):
            self.create(path=container_path)

        for obj in resources:
            if obj.created or obj.exists_in_repo(self):
                obj.created = True
                logger.debug(f'Object "{obj}" exists. Skipping.')
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

    def build_sparql_update(self, delete_graph: Graph = None, insert_graph: Graph = None) -> str:
        # go through each graph and update subjects with transaction IDs
        if delete_graph is not None:
            deletes = delete_graph.serialize(format='nt').decode('utf-8').strip()
        else:
            deletes = None

        if insert_graph is not None:
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

    @contextmanager
    def transaction(self, keep_alive: int = 90):
        logger.info('Creating transaction')
        try:
            response = self.post(self.repo.transaction_endpoint)
        except ConnectionError as e:
            raise TransactionError(f'Failed to create transaction: {e}') from e
        if response.status_code == 201:
            txn_client = TransactionClient.from_client(self)
            txn_client.begin(uri=response.headers['Location'], keep_alive=keep_alive)
            logger.info(f'Created transaction at {txn_client.transaction}')
            try:
                yield txn_client
            except RESTAPIException:
                txn_client.rollback()
            else:
                txn_client.commit()
            finally:
                # when we leave the transaction context, always
                # set the stop flag on the keep-alive ping
                txn_client.transaction.stop()
        else:
            raise TransactionError(f'Failed to create transaction: {response.status_code} {response.reason}')


class Transaction:
    def __init__(self, client: 'TransactionClient', uri: str, keep_alive=90, active: bool = True):
        self.uri = uri
        self.keep_alive = TransactionKeepAlive(client, keep_alive)
        self.active = active
        if self.active:
            self.keep_alive.start()

    def __str__(self):
        return self.uri

    @property
    def maintenance_uri(self):
        return os.path.join(self.uri, 'fcr:tx')

    @property
    def commit_uri(self):
        return os.path.join(self.uri, 'fcr:tx/fcr:commit')

    @property
    def rollback_uri(self):
        return os.path.join(self.uri, 'fcr:tx/fcr:rollback')

    def stop(self):
        """
        Stop the keep-alive thread and set the active flag to False. This should
        always be called before committing or rolling back a transaction.
        """
        self.keep_alive.stop()
        self.active = False


class TransactionClient(Client):
    @classmethod
    def from_client(cls, client: Client):
        return cls(
            repo=client.repo,
            structure=client.structure,
            auth=client.session.auth,
            server_cert=client.session.verify,
            ua_string=client.ua_string,
            on_behalf_of=client.delegated_user,
            load_binaries=client.load_binaries,
        )

    def __init__(self, repo: Repository, **kwargs):
        super().__init__(repo, **kwargs)
        self.transaction = None

    def request(self, method, url, **kwargs):
        # make sure the transaction keep-alive thread hasn't failed
        if self.transaction.keep_alive.failed.is_set():
            raise RuntimeError('Transaction keep-alive failed') from self.transaction.keep_alive.exception

        target_uri = self.insert_transaction_uri(url)
        return super().request(method, target_uri, **kwargs)

    def get_location(self, response: Response) -> Optional[str]:
        try:
            return self.remove_transaction_uri(response.headers['Location'])
        except KeyError:
            logger.warning('No Location header in response')
            return None

    def build_sparql_update(self, delete_graph: Graph = None, insert_graph: Graph = None) -> str:
        return super().build_sparql_update(
            delete_graph=self.insert_transaction_uri_for_graph(delete_graph),
            insert_graph=self.insert_transaction_uri_for_graph(insert_graph),
        )

    def get_description_uri(self, uri: str, response: Response = None) -> str:
        return self.remove_transaction_uri(super().get_description_uri(uri=uri, response=response))

    def insert_transaction_uri(self, uri: str) -> str:
        if uri.startswith(self.transaction.uri):
            return uri
        return self.transaction.uri + self.repo.repo_path(uri)

    def remove_transaction_uri(self, uri: str) -> str:
        if uri.startswith(self.transaction.uri):
            repo_path = uri[len(self.transaction.uri):]
            return self.repo.endpoint + repo_path
        else:
            return uri

    def insert_transaction_uri_for_graph(self, graph: Optional[Graph]) -> Optional[Graph]:
        if graph is None:
            return None
        new_graph = Graph()
        for s, p, o in graph:
            s_txn = URIRef(self.insert_transaction_uri(str(s)))
            new_graph.add((s_txn, p, o))
        return new_graph

    @property
    def active(self):
        return self.transaction and self.transaction.active

    def begin(self, uri: str, keep_alive: int = 90):
        self.transaction = Transaction(client=self, uri=uri, keep_alive=keep_alive)

    def maintain(self):
        logger.info(f'Maintaining transaction {self}')
        if not self.active:
            raise TransactionError(f'Cannot maintain inactive transaction: {self.transaction}')

        try:
            response = self.post(self.transaction.maintenance_uri)
        except ConnectionError as e:
            raise TransactionError(f'Failed to maintain transaction {self.transaction}: {e}') from e
        if response.status_code == 204:
            logger.info(f'Transaction {self} is active until {response.headers["Expires"]}')
        else:
            raise TransactionError(
                f'Failed to maintain transaction {self.transaction}: {response.status_code} {response.reason}'
            )

    def commit(self):
        logger.info(f'Committing transaction {self.transaction}')
        if not self.active:
            raise TransactionError(f'Cannot commit inactive transaction: {self.transaction}')

        self.transaction.stop()
        try:
            response = self.post(self.transaction.commit_uri)
        except ConnectionError as e:
            raise TransactionError(f'Failed to commit transaction {self.transaction}: {e}') from e
        if response.status_code == 204:
            logger.info(f'Committed transaction {self.transaction}')
            return response
        else:
            raise TransactionError(
                f'Failed to commit transaction {self.transaction}: {response.status_code} {response.reason}'
            )

    def rollback(self):
        logger.info(f'Rolling back transaction {self.transaction}')
        if not self.transaction.active:
            raise TransactionError(f'Cannot roll back inactive transaction: {self.transaction}')

        self.transaction.stop()
        try:
            response = self.post(self.transaction.rollback_uri)
        except ConnectionError as e:
            raise TransactionError(f'Failed to roll back transaction {self}: {e}') from e
        if response.status_code == 204:
            logger.info(f'Rolled back transaction {self.transaction}')
            return response
        else:
            raise TransactionError(
                f'Failed to roll back transaction {self.transaction}: {response.status_code} {response.reason}'
            )


# based on https://stackoverflow.com/a/12435256/5124907
class TransactionKeepAlive(threading.Thread):
    def __init__(self, txn_client: TransactionClient, interval: int):
        super().__init__(name='TransactionKeepAlive')
        self.txn_client = txn_client
        self.interval = interval
        self.stopped = threading.Event()
        self.failed = threading.Event()
        self.exception = None

    def run(self):
        while not self.stopped.wait(self.interval):
            try:
                self.txn_client.maintain()
            except TransactionError as e:
                # stop trying to maintain the transaction
                self.stop()
                # set the "failed" flag to communicate back to the main thread
                # that we were unable to maintain the transaction
                self.exception = e
                self.failed.set()

    def stop(self):
        self.stopped.set()


class TransactionError(Exception):
    pass
