import logging
import os
import threading
from base64 import urlsafe_b64encode
from collections import namedtuple
from contextlib import contextmanager
from enum import Enum
from http import HTTPStatus
from pathlib import Path
from typing import Any, Optional, List, Callable, NamedTuple, Union

import requests
from rdflib import Graph, URIRef, Literal
from requests import Response
from requests.auth import AuthBase
from requests.exceptions import ConnectionError
from urlobject import URLObject

logger = logging.getLogger(__name__)

OMIT_SERVER_MANAGED_TRIPLES = 'return=representation; omit="http://fedora.info/definitions/v4/repository#ServerManaged"'


def random_slug(length=6):
    return urlsafe_b64encode(os.urandom(length)).decode()


def paths_to_create(client: 'Client', path: Path) -> List[Path]:
    if client.path_exists(str(path)):
        return []
    to_create = [path]
    for ancestor in path.parents:
        if not client.path_exists(str(ancestor)):
            to_create.insert(0, ancestor)
    return to_create


def serialize(graph: Graph, **kwargs):
    logger.info('Including properties:')
    for _, p, o in graph:  # type: _, URIRef, Union[URIRef, Literal]
        pred = p.n3(namespace_manager=graph.namespace_manager)
        obj = o.n3(namespace_manager=graph.namespace_manager)
        logger.info(f'  {pred} {obj}')
    return graph.serialize(**kwargs)


# lightweight representation of a resource URI and URI of its description
# for RDFSources, in general the uri and description_uri will be the same
class ResourceURI(namedtuple('Resource', ['uri', 'description_uri'])):
    __slots__ = ()

    def __str__(self):
        return self.uri


class TypedText(NamedTuple):
    """Data object combining a string value and its media type,
    expressed as a MIME type string."""
    media_type: str
    value: str

    def __str__(self):
        return self.value

    def __bool__(self):
        return bool(self.value)

    def __len__(self):
        return len(self.value)


class ClientError(Exception):
    def __init__(self, response: Response, *args):
        super().__init__(*args)
        self.response = response
        self.status_code = self.response.status_code
        self.reason = self.response.reason or HTTPStatus(self.status_code).phrase

    def __str__(self):
        return f'{self.status_code} {self.reason}'


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


class Endpoint:
    """Conceptual entry point for a Fedora repository."""

    def __init__(self, url: str, default_path: str = '/', external_url: str = None):
        self.internal_url = URLObject(url)

        # default container path
        self.relpath = default_path
        if not self.relpath.startswith('/'):
            self.relpath = '/' + self.relpath

        if external_url is not None:
            self.external_url = URLObject(external_url)
        else:
            self.external_url: Optional[URLObject] = None

    @property
    def url(self):
        return self.external_url or self.internal_url

    def __contains__(self, item):
        return self.contains(item)

    def contains(self, uri: str) -> bool:
        """
        Returns ``True`` if the given URI string is contained within this
        repository, ``False`` otherwise. You may also use the builtin operator
        ``in`` to do this same check::

            endpoint = Endpoint(url='http://localhost:8080/fcrepo/rest')

            assert endpoint.contains('http://localhost:8080/fcrepo/rest/123')
            assert 'http://localhost:8080/fcrepo/rest/123' in endpoint

            assert not endpoint.contains('http://example.com/123')
            assert 'http://example.com/123' not in endpoint

        If ``external_url`` is set, checks that too::

            endpoint = Endpoint(
                url='http://localhost:8080/fcrepo/rest',
                external_url='https://repo.example.net',
            )

            assert 'https://repo.example.net/123' in endpoint
            assert 'http://localhost:8080/fcrepo/rest/123' in endpoint
        """
        return uri.startswith(self.internal_url) \
            or (self.external_url is not None and uri.startswith(self.external_url))

    def repo_path(self, resource_uri: Optional[str]) -> Optional[str]:
        """
        Returns the repository path for the given resource URI, i.e. the
        path with either the ``url`` or ``external_url`` (if defined)
        removed. For example::

            endpoint = Endpoint(url='http://localhost:8080/fcrepo/rest')
            assert endpoint.repo_path('http://localhost:8080/fcrepo/rest/obj/123') == '/obj/123'
        """
        if resource_uri is None:
            return None
        elif self.external_url:
            return resource_uri.replace(self.external_url, '')
        else:
            return resource_uri.replace(self.url, '')

    @property
    def transaction_endpoint(self) -> str:
        return os.path.join(self.url, 'fcr:tx')


class RepositoryStructure(Enum):
    FLAT = 0
    HIERARCHICAL = 1


class SessionHeaderAttribute:
    """Descriptor that maps an attribute to a session header name. Requires
    the instance to have a session attribute whose value is a mapping that
    supports the methods get and update, plus the del operator. Example::

        from requests import Session

        class Foo:
            ua_string = SessionHeaderAttribute('User-Agent')

            def __init__(self):
                self.session = Session()

        # initially not set
        foo = Foo()
        assert 'User-Agent' not in foo.session.headers

        # set the header
        foo.ua_string = 'MyClient/1.0.0'
        assert foo.session.headers['User-Agent'] == 'MyClient/1.0.0'

        # change the header
        foo.ua_string = 'OtherAgent/2.0.0'
        assert foo.session.headers['User-Agent'] == 'OtherAgent/2.0.0'

        # remove the header
        del foo.ua_string
        assert 'User-Agent' not in foo.session.headers
    """

    def __init__(self, header_name: str):
        self.header_name = header_name

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.session.headers.get(self.header_name, None)

    def __set__(self, instance, value):
        if value is not None:
            instance.session.headers.update({self.header_name: str(value)})

    def __delete__(self, instance):
        try:
            del instance.session.headers[self.header_name]
        except KeyError:
            pass


def build_sparql_update(delete_graph: Graph = None, insert_graph: Graph = None) -> str:
    if delete_graph is not None and len(delete_graph) > 0:
        deletes = delete_graph.serialize(format='nt').strip()
    else:
        deletes = None

    if insert_graph is not None and len(insert_graph) > 0:
        inserts = insert_graph.serialize(format='nt').strip()
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


class Client:
    """HTTP client for interacting with a Fedora repository."""
    ua_string = SessionHeaderAttribute('User-Agent')
    delegated_user = SessionHeaderAttribute('On-Behalf-Of')
    forwarded_host = SessionHeaderAttribute('X-Forwarded-Host')
    forwarded_protocol = SessionHeaderAttribute('X-Forwarded-Proto')

    def __init__(
            self,
            endpoint: Endpoint,
            structure: RepositoryStructure = RepositoryStructure.FLAT,
            auth: AuthBase = None,
            server_cert: str = None,
            ua_string: str = None,
            on_behalf_of: str = None,
            load_binaries: bool = True,
    ):
        self.endpoint = endpoint
        self.structure = structure
        self.load_binaries = load_binaries

        self.session = requests.Session()
        self.session.auth = auth
        if server_cert is not None:
            self.session.verify = server_cert

        # set session-wide headers
        self.ua_string = ua_string
        self.delegated_user = on_behalf_of
        if self.endpoint.external_url is not None:
            if self.endpoint.external_url.port:
                # fcrepo expects hostname and port in the X-Forwarded-Host header
                self.forwarded_host = f'{self.endpoint.external_url.hostname}:{self.endpoint.external_url.port}'
            else:
                self.forwarded_host = self.endpoint.external_url.hostname
            self.forwarded_protocol = self.endpoint.external_url.scheme

        # set creator strategy for the repository
        if self.structure is RepositoryStructure.HIERARCHICAL:
            self.creator = HierarchicalCreator(self)
        elif self.structure is RepositoryStructure.FLAT:
            self.creator = FlatCreator(self)
        else:
            raise RuntimeError(f'Unknown STRUCTURE value: {structure}')

    def request(self, method: str, url: str, **kwargs) -> Response:
        """Send an HTTP request using the configured session. Additional
        keyword arguments are passed to the underlying Session's ``request``
        method."""
        logger.debug(f'{method} {url}')
        response = self.session.request(method, url, **kwargs)
        logger.debug(f'{response.status_code} {response.reason}')
        return response

    def post(self, url: str, **kwargs) -> Response:
        """Send an HTTP POST request using the configured session."""
        return self.request('POST', url, **kwargs)

    def put(self, url: str, **kwargs) -> Response:
        """Send an HTTP PUT request using the configured session."""
        return self.request('PUT', url, **kwargs)

    def patch(self, url: str, **kwargs) -> Response:
        """Send an HTTP PATCH request using the configured session."""
        return self.request('PATCH', url, **kwargs)

    def head(self, url: str, **kwargs) -> Response:
        """Send an HTTP HEAD request using the configured session."""
        return self.request('HEAD', url, **kwargs)

    def get(self, url: str, **kwargs) -> Response:
        """Send an HTTP DELETE request using the configured session."""
        return self.request('GET', url, **kwargs)

    def delete(self, url: str, **kwargs) -> Response:
        """Send an HTTP DELETE request using the configured session."""
        return self.request('DELETE', url, **kwargs)

    def get_description(
            self,
            url: str,
            accept: str = 'application/n-triples',
            include_server_managed: bool = True
    ) -> TypedText:
        headers = {
            'Accept': accept,
        }
        if not include_server_managed:
            headers['Prefer'] = OMIT_SERVER_MANAGED_TRIPLES
        response = self.get(url, headers=headers, stream=True)
        if not response.ok:
            logger.error(f"Unable to get {headers['Accept']} representation of {url}")
            raise ClientError(response=response)
        return TypedText(response.headers['Content-Type'], response.text)

    def get_graph(self, url: str, include_server_managed: bool = True) -> Graph:
        text = self.get_description(url, include_server_managed=include_server_managed)
        graph = Graph()
        graph.parse(data=text.value, format=text.media_type)
        return graph

    def get_description_uri(self, uri: str, response: Response = None) -> str:
        if response is not None:
            if not response.ok:
                raise ClientError(response)
            try:
                return response.links['describedby']['url']
            except KeyError:
                return uri

        # only if we didn't get a response argument do we make a request
        return self.get_description_uri(uri, response=self.head(uri))

    def is_reachable(self):
        try:
            response = self.head(self.endpoint.url)
            return response.status_code == 200
        except requests.exceptions.ConnectionError as e:
            logger.error(str(e))
            return False

    def test_connection(self):
        # test connection to fcrepo
        logger.debug(f"Endpoint = {self.endpoint.url}")
        logger.debug(f"Default container path = {self.endpoint.relpath}")
        logger.info(f"Testing connection to {self.endpoint.url}")
        if self.is_reachable():
            logger.info("Connection successful.")
        else:
            raise ConnectionError(f'Unable to connect to {self.endpoint.url}')

    def exists(self, uri: str, **kwargs) -> bool:
        response = self.head(uri, **kwargs)
        return response.status_code == HTTPStatus.OK

    def path_exists(self, path: str, **kwargs) -> bool:
        return self.exists(self.endpoint.url + path, **kwargs)

    def get_location(self, response: Response) -> Optional[str]:
        try:
            return response.headers['Location']
        except KeyError:
            logger.warning('No Location header in response')
            return None

    def create(
            self,
            path: str = None,
            url: str = None,
            container_path: str = None,
            slug: str = None,
            **kwargs,
    ) -> ResourceURI:
        if url is not None:
            response = self.put(url, **kwargs)
        elif path is not None:
            response = self.put(self.endpoint.url + path, **kwargs)
        else:
            if 'headers' not in kwargs:
                kwargs['headers'] = {}
            if slug is not None:
                kwargs['headers']['Slug'] = slug
            container_uri = self.endpoint.url + (container_path or self.endpoint.relpath)
            response = self.post(container_uri, **kwargs)

        if response.status_code == HTTPStatus.CREATED:
            created_uri = self.get_location(response) or url
            description_uri = self.get_description_uri(created_uri, response)

            return ResourceURI(created_uri, description_uri)
        else:
            raise ClientError(response)

    def create_at_path(self, target_path: Path, graph: Graph = None):
        all_paths = paths_to_create(self, target_path)

        if len(all_paths) == 0:
            logger.info(f'{target_path} already exists')
            return

        resource = None
        for path in all_paths:
            logger.info(f'Creating {path}')
            if path == target_path and graph:
                resource = self.create(
                    path=str(path),
                    headers={
                        'Content-Type': 'text/turtle'
                    },
                    data=serialize(graph, format='turtle')
                )
            else:
                resource = self.create(path=str(path))

            logger.info(f'Created {resource}')

        return resource

    def create_in_container(self, container_path: Path, graph: Graph = None):
        if not self.path_exists(str(container_path)):
            logger.error(f'Container path "{container_path}" not found')
            return
        if graph:
            resource = self.create(
                container_path=str(container_path),
                headers={
                    'Content-Type': 'text/turtle'
                },
                data=serialize(graph, format='turtle')
            )
        else:
            resource = self.create(container_path=str(container_path))

        logger.info(f'Created {resource}')
        return resource

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

    def put_graph(self, url, graph: Graph) -> Response:
        return self.put(
            self.get_description_uri(url),
            headers={
                'Content-Type': 'application/n-triples',
            },
            data=graph.serialize(format='application/n-triples')
        )

    def patch_graph(self, url, deletes: Graph, inserts: Graph) -> Response:
        sparql_update = build_sparql_update(deletes, inserts)
        logger.debug(sparql_update)
        return self.patch(
            url,
            headers={
                'Content-Type': 'application/sparql-update'
            },
            data=sparql_update,
        )

    @contextmanager
    def transaction(self, keep_alive: int = 90):
        logger.info('Creating transaction')
        try:
            response = self.post(self.endpoint.transaction_endpoint)
        except ConnectionError as e:
            raise TransactionError(f'Failed to create transaction: {e}') from e
        if response.status_code == HTTPStatus.CREATED:
            txn_client = TransactionClient.from_client(self)
            txn_client.begin(uri=response.headers['Location'], keep_alive=keep_alive)
            logger.info(f'Created transaction at {txn_client.tx}')
            try:
                yield txn_client
            except ClientError:
                txn_client.rollback()
                raise
            else:
                txn_client.commit()
            finally:
                # when we leave the transaction context, always
                # set the stop flag on the keep-alive ping
                txn_client.tx.stop()
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
    """HTTP client that transparently handles translating requests and responses
    sent during a Fedora transaction. Adds and removes the transaction identifier
    from URIs in graphs sent or returned. Adjusts the request URIs to include the
    transaction identifier."""

    @classmethod
    def from_client(cls, client: Client):
        """Build a TransactionClient from a regular Client object."""
        return cls(
            endpoint=client.endpoint,
            structure=client.structure,
            auth=client.session.auth,
            server_cert=client.session.verify,
            ua_string=client.ua_string,
            on_behalf_of=client.delegated_user,
            load_binaries=client.load_binaries,
        )

    def __init__(self, endpoint: Endpoint, **kwargs):
        super().__init__(endpoint, **kwargs)
        self.tx = None

    def request(self, method, url, **kwargs):
        # make sure the transaction keep-alive thread hasn't failed
        if self.tx.keep_alive.failed.is_set():
            raise RuntimeError('Transaction keep-alive failed') from self.tx.keep_alive.exception

        request_url = str(self.insert_transaction_uri(URIRef(url)))
        return super().request(method, request_url, **kwargs)

    def get_location(self, response: Response) -> Optional[str]:
        """Removes the transaction id from the ``Location`` header returned by requests
        to create resources."""
        try:
            return str(self.remove_transaction_uri(URIRef(response.headers['Location'])))
        except KeyError:
            logger.warning('No Location header in response')
            return None

    def get_description(
            self,
            url: str,
            accept: str = 'application/n-triples',
            include_server_managed: bool = True,
    ) -> TypedText:
        text = super().get_description(
            url=str(self.insert_transaction_uri(URIRef(url))),
            accept=accept,
            include_server_managed=include_server_managed,
        )
        graph = self.remove_transaction_uri_for_graph(Graph().parse(data=text.value, format=text.media_type))
        return TypedText(text.media_type, graph.serialize(format=text.media_type))

    def put_graph(self, url, graph: Graph) -> Response:
        return super().put_graph(
            url=url,
            graph=self.insert_transaction_uri_for_graph(graph),
        )

    def patch_graph(self, url, deletes: Graph, inserts: Graph) -> Response:
        return super().patch_graph(
            url=url,
            deletes=self.insert_transaction_uri_for_graph(deletes),
            inserts=self.insert_transaction_uri_for_graph(inserts),
        )

    def get_description_uri(self, uri: str, response: Response = None) -> str:
        return str(self.remove_transaction_uri(URIRef(super().get_description_uri(uri=uri, response=response))))

    def insert_transaction_uri(self, uri: Any) -> Any:
        if not isinstance(uri, URIRef):
            return uri
        if uri.startswith(self.tx.uri):
            return uri
        return URIRef(self.tx.uri + self.endpoint.repo_path(uri))

    def remove_transaction_uri(self, uri: Any) -> Any:
        if not isinstance(uri, URIRef):
            return uri
        if uri.startswith(self.tx.uri):
            return URIRef(uri.replace(self.tx.uri, self.endpoint.url))
        else:
            return uri

    def insert_transaction_uri_for_graph(self, graph: Optional[Graph]) -> Optional[Graph]:
        if graph is None:
            return None
        new_graph = Graph()
        for s, p, o in graph:
            s_txn = self.insert_transaction_uri(s)
            o_txn = self.insert_transaction_uri(o)
            new_graph.add((s_txn, p, o_txn))
        return new_graph

    def remove_transaction_uri_for_graph(self, graph: Optional[Graph]) -> Optional[Graph]:
        if graph is None:
            return None
        new_graph = Graph()
        for s, p, o in graph:
            s_txn = self.remove_transaction_uri(s)
            o_txn = self.remove_transaction_uri(o)
            new_graph.add((s_txn, p, o_txn))
        return new_graph

    def transaction(self, keep_alive: int = 90):
        raise TransactionError('Cannot nest transactions')

    @property
    def active(self):
        return self.tx and self.tx.active

    def begin(self, uri: str, keep_alive: int = 90):
        self.tx = Transaction(client=self, uri=uri, keep_alive=keep_alive)

    def maintain(self):
        logger.info(f'Maintaining transaction {self}')
        if not self.active:
            raise TransactionError(f'Cannot maintain inactive transaction: {self.tx}')

        try:
            response = self.post(self.tx.maintenance_uri)
        except ConnectionError as e:
            raise TransactionError(f'Failed to maintain transaction {self.tx}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Transaction {self} is active until {response.headers["Expires"]}')
        else:
            raise TransactionError(
                f'Failed to maintain transaction {self.tx}: {response.status_code} {response.reason}'
            )

    def commit(self):
        logger.info(f'Committing transaction {self.tx}')
        if not self.active:
            raise TransactionError(f'Cannot commit inactive transaction: {self.tx}')

        self.tx.stop()
        try:
            response = self.post(self.tx.commit_uri)
        except ConnectionError as e:
            raise TransactionError(f'Failed to commit transaction {self.tx}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Committed transaction {self.tx}')
            return response
        else:
            raise TransactionError(
                f'Failed to commit transaction {self.tx}: {response.status_code} {response.reason}'
            )

    def rollback(self):
        logger.info(f'Rolling back transaction {self.tx}')
        if not self.tx.active:
            raise TransactionError(f'Cannot roll back inactive transaction: {self.tx}')

        self.tx.stop()
        try:
            response = self.post(self.tx.rollback_uri)
        except ConnectionError as e:
            raise TransactionError(f'Failed to roll back transaction {self}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Rolled back transaction {self.tx}')
            return response
        else:
            raise TransactionError(
                f'Failed to roll back transaction {self.tx}: {response.status_code} {response.reason}'
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
