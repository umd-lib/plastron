import logging
from http import HTTPStatus
from pathlib import Path
from typing import Optional, Any, Callable

from rdflib import Graph
from requests import Session, Response, ConnectionError
from requests.auth import AuthBase

from plastron.client.endpoint import Endpoint
from plastron.client.utils import SessionHeaderAttribute, TypedText, OMIT_SERVER_MANAGED_TRIPLES, ResourceURI, \
    serialize, build_sparql_update

logger = logging.getLogger(__name__)


class Client:
    """HTTP client for interacting with a Fedora repository."""
    ua_string = SessionHeaderAttribute('User-Agent')
    """`User-Agent` header value"""
    delegated_user = SessionHeaderAttribute('On-Behalf-Of')
    """`On-Behalf-Of` header value"""
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

        Returns a `plastron.client.utils.TypedText` object containing the response
        body.

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
        except RuntimeError as e:
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
        """Checks if a `HEAD` request to the given `uri` responds with a
        status code less than 400 (i.e., a 1xx, 2xx, or 3xx response)."""
        return self.head(uri, **kwargs).ok

    def path_exists(self, path: str, **kwargs) -> bool:
        """Checks whether the repository path given `path` exists on the
        configured `endpoint`. Uses the `exists` method to do the actual
        check."""
        return self.exists(self.endpoint.url + path, **kwargs)

    def paths_to_create(self, path: Path) -> list[Path]:
        """Return a list of path prefixes in `path` that need to be created
        before creating `path` (i.e., they do not exist in the repository that
        `client` is configured to work with). This list is ordered from shortest
        to longest prefix."""

        if self.path_exists(str(path)):
            return []
        to_create = [path]
        for ancestor in path.parents:
            if not self.path_exists(str(ancestor)):
                to_create.insert(0, ancestor)
        return to_create

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
        all_paths = self.paths_to_create(target_path)

        if len(all_paths) == 0:
            logger.info(f'{target_path} already exists')
            return None

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
            return None
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
