import logging
import os
import threading
from base64 import urlsafe_b64encode
from collections import namedtuple
from contextlib import contextmanager
from http import HTTPStatus
from pathlib import Path
from typing import Any, Optional, Callable, NamedTuple

import requests
from rdflib import Graph, URIRef, Literal
from requests import Response, Session
from requests.auth import AuthBase
from requests.exceptions import ConnectionError
from urlobject import URLObject

logger = logging.getLogger(__name__)

OMIT_SERVER_MANAGED_TRIPLES = 'return=representation; omit="http://fedora.info/definitions/v4/repository#ServerManaged"'


def random_slug(length: int = 6) -> str:
    """Generate a URL-safe random string of characters. Uses `os.urandom()`
    as its source of entropy."""
    return urlsafe_b64encode(os.urandom(length)).decode()


def paths_to_create(client: 'Client', path: Path) -> list[Path]:
    if client.path_exists(str(path)):
        return []
    to_create = [path]
    for ancestor in path.parents:
        if not client.path_exists(str(ancestor)):
            to_create.insert(0, ancestor)
    return to_create


def serialize(graph: Graph, **kwargs):
    logger.info('Including properties:')
    for _, p, o in graph:  # type: _, URIRef, URIRef | Literal
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
    expressed as a MIME type string.

    ```pycon
    >>> json_data = TypedText('application/json', '{"foo": 1, "bar": "two"}')
    >>> json_data
    TypedText(media_type='application/json', value='{"foo": 1, "bar": "two"}')
    ```

    Supports `str()`, `len()`, and `bool()`. Returns the string value, the
    length of the string value, and the boolean cast of the string value,
    respectively:

    ```pycon
    >>> str(json_data)
    '{"foo": 1, "bar": "two"}'

    >>> len(json_data)
    24

    >>> bool(json_data)
    True
    ```

    Two `TypedText` objects are only equal if both the string value
    and the media type match:

    ```pycon
    >>> json_data = TypedText('application/json', '{"foo": 1, "bar": "two"}')
    >>> text_data = TypedText('text/plain', '{"foo": 1, "bar": "two"}')

    >>> json_data.value == text_data.value
    True

    >>> json_data == text_data
    False
    ```
    """

    media_type: str
    """MIME type, e.g. "text/plain" or "application/ld+json" """

    value: str
    """string value"""

    def __str__(self):
        return self.value

    def __bool__(self):
        return bool(self.value)

    def __len__(self):
        return len(self.value)


class ClientError(Exception):
    """Raised when a `Client` receives an HTTP error response (4xx or 5xx)."""
    def __init__(self, response: Response, *args):
        super().__init__(*args)

        self.response: Response = response
        """The Requests `Response` object from the failed request."""

        self.status_code = self.response.status_code
        """The numeric HTTP status code (e.g., 404) for the failed request."""

        self.reason = self.response.reason or HTTPStatus(self.status_code).phrase
        """The reason phrase (e.g., "Not Found") for the failed request. If
        the `response` does not have a reason code, use the standard status
        phrase from the built-in
        [`HTTPStatus`](https://docs.python.org/3.8/library/http.html#http.HTTPStatus)
        enumeration corresponding to the `status_code`."""

    def __str__(self):
        return f'{self.status_code} {self.reason}'


class Endpoint:
    """Conceptual entry point for a Fedora repository."""

    def __init__(self, url: str, default_path: str = '/', external_url: str = None):
        self.internal_url = URLObject(url)

        self.relpath = default_path
        """Default container path"""

        if not self.relpath.startswith('/'):
            self.relpath = '/' + self.relpath

        self.external_url: URLObject = URLObject(external_url) if external_url is not None else None

    @property
    def url(self) -> URLObject:
        """Endpoint URL. If `external_url` is set, returns that. Otherwise, returns
        the `internal_url`."""
        return self.external_url or self.internal_url

    def __contains__(self, item):
        return self.contains(item)

    def contains(self, uri: str) -> bool:
        """
        Returns `True` if the given URI string is contained within this
        repository, `False` otherwise. You may also use the builtin operator
        `in` to do this same check::

        ```pycon
        >>> endpoint = Endpoint(url='http://localhost:8080/fcrepo/rest')

        >>> endpoint.contains('http://localhost:8080/fcrepo/rest/123')
        True

        >>> 'http://localhost:8080/fcrepo/rest/123' in endpoint
        True

        >>> endpoint.contains('http://example.com/123')
        False

        >>> 'http://example.com/123' in endpoint
        False
        ```

        If `external_url` is set, checks that too::

        ```python
        >>> endpoint = Endpoint(
        ...     url='http://localhost:8080/fcrepo/rest',
        ...     external_url='https://repo.example.net',
        ... )

        >>> 'https://repo.example.net/123' in endpoint
        True

        >>> 'http://localhost:8080/fcrepo/rest/123' in endpoint
        True
        ```
        """
        return uri.startswith(self.internal_url) \
            or (self.external_url is not None and uri.startswith(self.external_url))

    def repo_path(self, resource_uri: Optional[str]) -> Optional[str]:
        """
        Returns the repository path for the given resource URI, i.e. the
        path with either the ``url`` or ``external_url`` (if defined)
        removed. For example:

        ```pycon
        >>> endpoint = Endpoint(url='http://localhost:8080/fcrepo/rest')

        >>> endpoint.repo_path('http://localhost:8080/fcrepo/rest/obj/123')
        '/obj/123'
        ```
        """
        if resource_uri is None:
            return None
        elif self.external_url:
            return resource_uri.replace(self.external_url, '')
        else:
            return resource_uri.replace(self.url, '')

    @property
    def transaction_endpoint(self) -> str:
        """Send an HTTP POST request to this URL to create a new transaction."""
        return os.path.join(self.url, 'fcr:tx')


class SessionHeaderAttribute:
    """Descriptor that maps an attribute to a session header name. Requires
    the instance to have a `session` attribute with a `headers` attribute whose
    value is a mapping that supports the methods `get()` and `update()`, plus
    the `del` operator. For example:

    ```python
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
    ```
    """

    def __init__(self, header_name: str):
        self.header_name = header_name
        """The HTTP header name"""

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
    """Build a SPARQL Update Query given the two graphs:

    * If there are no deletes (i.e., `delete_graph` contains no triples, or
      is set to `None`), returns an `INSERT DATA { ... }` statement;
    * If there are no inserts (i.e., `insert_graph` contains no triples, or
      is set to `None`), returns a `DELETE DATA { ... }` statement;
    * If there are both deletes and inserts, returns a full `DELETE { ... } INSERT { ... }
      WHERE {}` statement (the `WHERE` clause is always empty);
    * If there are neither inserts nor deletes, returns the empty string."""
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
    """`User-Agent` header value"""
    delegated_user = SessionHeaderAttribute('On-Behalf-Of')
    """`On-Behalf-Of` header value"""
    forwarded_host = SessionHeaderAttribute('X-Forwarded-Host')
    """`X-Forwarded-Host` header value. This is automatically set if the
    `endpoint` has an `external_url`."""
    forwarded_protocol = SessionHeaderAttribute('X-Forwarded-Proto')
    """`X-Forwarded-Proto` header value. This is automatically set if the
    `endpoint` has an `external_url`."""
    session: Session
    """Underlying Requests library
    [Session object](https://requests.readthedocs.io/en/latest/user/advanced/#session-objects),
    or a subclass thereof"""

    def __init__(
        self,
        endpoint: Endpoint,
        auth: AuthBase = None,
        server_cert: str = None,
        ua_string: str = None,
        on_behalf_of: str = None,
        load_binaries: bool = True,
        session: Session = None,
    ):
        self.endpoint: Endpoint = endpoint
        """Fedora repository endpoint"""
        self.load_binaries: bool = load_binaries

        if session is None:
            # defaults to a basic requests.Session object
            self.session = Session()
        else:
            # otherwise, use the session object as is
            self.session = session

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

    def request(self, method: str, url: str, **kwargs) -> Response:
        """Send an HTTP request using the configured `session`. Additional
        keyword arguments are passed to the underlying `session.request()`
        method."""
        logger.debug(f'{method} {url}')
        try:
            response = self.session.request(method, url, **kwargs)
        except ConnectionError as e:
            message = ' '.join(str(arg) for arg in e.args)
            logger.error(message)
            raise RuntimeError(f'Connection error: {message}') from e
        # be aware of an optional requests cache
        if hasattr(response, 'from_cache'):
            if response.from_cache:
                logger.debug(f'Cache hit for {url}')
            else:
                logger.debug(f'Cache miss for {url}')
        reason = response.reason or HTTPStatus(response.status_code).phrase
        logger.debug(f'{response.status_code} {reason}')
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
        """Get the content at `url` by issuing an HTTP GET request. Defaults to
        sending an `Accept: application/n-triples` header, but that can be
        changed by setting the `accept` argument. It also by default includes
        all the server-managed triples. These can be suppressed by setting the
        `include_server_managed` argument to `False`.

        Returns a `TypedText` object containing the response body.

        Raises a `ClientError` if it does not get a success response from the
        server."""

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
        """Get the `rdflib.Graph` object representing the resource at `url`."""
        text = self.get_description(url, include_server_managed=include_server_managed)
        graph = Graph()
        graph.parse(data=text.value, format=text.media_type)
        return graph

    def get_description_uri(self, uri: str, response: Response = None) -> str:
        """Check the `response` for a `Link` header with `rel="describedby"`. If
        present, returns that URI. Otherwise, assume the resource describes
        itself, and return the original `uri` argument.

        If no `response` is given, make an HTTP HEAD request to the `uri`.

        Raises a `ClientError` if `response` is not a success response."""
        if response is not None:
            if not response.ok:
                raise ClientError(response)
            try:
                return response.links['describedby']['url']
            except KeyError:
                return uri

        # only if we didn't get a response argument do we make a request
        return self.get_description_uri(uri, response=self.head(uri))

    def is_reachable(self) -> bool:
        """Returns `True` if an HTTP HEAD request to the configured `endpoint`
        yields a non-error response, and `False` otherwise."""
        try:
            return self.head(self.endpoint.url).ok
        except requests.exceptions.ConnectionError as e:
            logger.error(str(e))
            return False

    def test_connection(self):
        """Test the connection to the repository using `is_reachable()`. If
        it returns false, raises a `ConnectionError`."""
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
        """Return the value of the `Location` HTTP header in `response`,
        or `None` if there is no such header."""
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

    def create_all(self, container_path: str, resources: list[Any], name_function: Callable = None):
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
    def __init__(self, client: 'TransactionClient', uri: str, keep_alive: int = 90, active: bool = True):
        self.uri: str = uri
        self.keep_alive: TransactionKeepAlive = TransactionKeepAlive(client, keep_alive)
        self.active: bool = active
        if self.active:
            self.keep_alive.start()

    def __str__(self):
        return self.uri

    @property
    def maintenance_url(self):
        """Send a POST request to this URL to keep the transaction alive."""
        return os.path.join(self.uri, 'fcr:tx')

    @property
    def commit_url(self):
        """Send a POST request to this URL to commit the transaction."""
        return os.path.join(self.uri, 'fcr:tx/fcr:commit')

    @property
    def rollback_url(self):
        """Send a POST request to this URL to roll back the transaction."""
        return os.path.join(self.uri, 'fcr:tx/fcr:rollback')

    def stop(self):
        """
        Stop the keep-alive thread and set the `active` flag to `False`. This should
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
        """Build a `TransactionClient` from a regular `Client` object."""
        return cls(
            endpoint=client.endpoint,
            auth=client.session.auth,
            server_cert=client.session.verify,
            ua_string=client.ua_string,
            on_behalf_of=client.delegated_user,
            load_binaries=client.load_binaries,
        )

    def __init__(self, endpoint: Endpoint, **kwargs):
        super().__init__(endpoint, **kwargs)
        self.tx: Optional[Transaction] = None
        """The transaction"""

    def request(self, method: str, url: str, **kwargs) -> Response:
        """Makes sure the transaction keep-alive thread hasn't failed, and inserts the transaction
        id into the request URL. Then calls the `Client.request()` method with the same arguments.

        Raises a `RuntimeError` if the transaction keep-alive thread has failed."""
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
        """If `uri` is in this client's `endpoint` but does not contain the current transaction ID,
        return a modified URI with the transaction ID added to it. Otherwise, return the `uri` argument
        as-is."""
        if not isinstance(uri, URIRef):
            return uri
        if uri.startswith(self.tx.uri):
            return uri
        if uri in self.endpoint:
            return URIRef(self.tx.uri + self.endpoint.repo_path(uri))
        return uri

    def remove_transaction_uri(self, uri: Any) -> Any:
        """If `uri` contains the current transaction ID, return a modified URI with the transaction ID
        removed. Otherwise, return the `uri` argument as-is."""
        if not isinstance(uri, URIRef):
            return uri
        if uri.startswith(self.tx.uri):
            return URIRef(uri.replace(self.tx.uri, self.endpoint.url))
        return uri

    def insert_transaction_uri_for_graph(self, graph: Optional[Graph]) -> Optional[Graph]:
        if graph is None:
            return None
        for s, p, o in graph:
            s_txn = self.insert_transaction_uri(s)
            o_txn = self.insert_transaction_uri(o)
            # swap the triple if either the subject or object is changed
            if s != s_txn or o != o_txn:
                graph.add((s_txn, p, o_txn))
                graph.remove((s, p, o))
        return graph

    def remove_transaction_uri_for_graph(self, graph: Optional[Graph]) -> Optional[Graph]:
        if graph is None:
            return None
        for s, p, o in graph:
            s_txn = self.remove_transaction_uri(s)
            o_txn = self.remove_transaction_uri(o)
            # swap the triple if either the subject or object is changed
            if s != s_txn or o != o_txn:
                graph.add((s_txn, p, o_txn))
                graph.remove((s, p, o))
        return graph

    def transaction(self, keep_alive: int = 90):
        """Immediately raises a `TransactionError`, since you cannot nest transactions."""
        raise TransactionError('Cannot nest transactions')

    @property
    def active(self):
        """Whether a transaction is set and active."""
        return self.tx and self.tx.active

    def begin(self, uri: str, keep_alive: int = 90):
        """Create a `Transaction` object and assign it to `tx`."""
        self.tx = Transaction(client=self, uri=uri, keep_alive=keep_alive)

    def maintain(self):
        """Sends an empty POST request to the `Transaction.maintenance_url` to keep it alive.
        Raises a `TransactionError` if the transaction is inactive, or there is a connection error
        or non-OK HTTP response from the repository server."""
        logger.info(f'Maintaining transaction {self}')
        if not self.active:
            raise TransactionError(f'Cannot maintain inactive transaction: {self.tx}')

        try:
            response = self.post(self.tx.maintenance_url)
        except ConnectionError as e:
            raise TransactionError(f'Failed to maintain transaction {self.tx}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Transaction {self} is active until {response.headers["Expires"]}')
        else:
            raise TransactionError(
                f'Failed to maintain transaction {self.tx}: {response.status_code} {response.reason}'
            )

    def commit(self):
        """Commits the transaction. Raises a `TransactionError` if the transaction is
        inactive, or there is a connection error or non-OK HTTP response from the repository
        server."""
        logger.info(f'Committing transaction {self.tx}')
        if not self.active:
            raise TransactionError(f'Cannot commit inactive transaction: {self.tx}')

        self.tx.stop()
        try:
            response = self.post(self.tx.commit_url)
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
        """Rolls back the transaction. Raises a `TransactionError` if the transaction is
        inactive, or there is a connection error or non-OK HTTP response from the repository
        server."""
        logger.info(f'Rolling back transaction {self.tx}')
        if not self.tx.active:
            raise TransactionError(f'Cannot roll back inactive transaction: {self.tx}')

        self.tx.stop()
        try:
            response = self.post(self.tx.rollback_url)
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
    """Thread to run in the background while a long-running transaction is being
    processed, to ensure that the transaction does not time out due to inactivity."""
    def __init__(self, txn_client: TransactionClient, interval: int):
        """Create a transaction keep-alive thread."""
        super().__init__(name='TransactionKeepAlive')
        self.txn_client: TransactionClient = txn_client
        """The transaction client."""

        self.interval: int = interval
        """Time between transaction maintenance requests."""

        self.stopped: threading.Event = threading.Event()
        """Flag indicating whether this transaction has been stopped."""

        self.failed: threading.Event = threading.Event()
        """Flag indicating whether this transaction has failed."""

        self.exception: Optional[TransactionError] = None
        """If this transaction could not be maintained, this holds the
        raised `TransactionError`."""

    def run(self):
        """Send a transaction maintenance request every `interval` seconds.
        If there is a `TransactionError` raised, set the `stopped` and `failed`
        flags on this thread, and store the raised exception as `exception`."""
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
        """Set the `stopped` flag on this thread."""
        self.stopped.set()


class TransactionError(Exception):
    """Raised when a transaction fails."""
    pass
