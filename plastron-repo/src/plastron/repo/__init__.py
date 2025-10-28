import logging
from contextlib import contextmanager
from http import HTTPStatus
from io import BytesIO
from typing import Optional, Type, TypeVar, Iterator, Union
from uuid import uuid4

import yaml
from math import inf
from rdflib import URIRef, Namespace
from requests import Response
from requests.auth import AuthBase
from urlobject import URLObject

from plastron.client import Client, Endpoint, ClientError
from plastron.client.auth import get_authenticator
from plastron.rdfmapping.graph import TrackChangesGraph
from plastron.rdfmapping.resources import RDFResourceBase, RDFResourceType

logger = logging.getLogger(__name__)
ldp = Namespace('http://www.w3.org/ns/ldp#')


def mint_fragment_identifier() -> str:
    return str(uuid4())


ResourceType = TypeVar('ResourceType', bound='RepositoryResource')


class Repository:
    @classmethod
    def from_config_file(cls, filename: str) -> 'Repository':
        with open(filename) as file:
            return cls.from_config(config=yaml.safe_load(file).get('REPOSITORY', {}))

    @classmethod
    def from_config(cls, config: dict[str, str]) -> 'Repository':
        endpoint = Endpoint(
            url=config['REST_ENDPOINT'],
            default_path=config.get('RELPATH', '/'),
            external_url=config.get('REPO_EXTERNAL_URL', None)
        )
        client = Client(endpoint=endpoint, auth=get_authenticator(config), server_cert=config.get('SERVER_CERT', None))
        return cls(client=client)

    @classmethod
    def from_url(cls, url: str, auth: AuthBase = None) -> 'Repository':
        endpoint = Endpoint(url=url)
        client = Client(endpoint=endpoint, auth=auth)
        return cls(client=client)

    def __init__(self, client: Client):
        self._client = client
        self.endpoint = client.endpoint
        self._txn_client = None

    @property
    def client(self):
        return self._txn_client or self._client

    def get_resource(self, path: str, resource_class: Type[ResourceType] = None) -> ResourceType:
        """Get an object representing a resource at a particular path with this repository.

        By default, returns an object of type `RepositoryResource`, but you may pass a different
        resource class in the `resource_class` parameter. That class must support a constructor
        with keyword arguments `repo` and `path`.

        :return: a `RepositoryResource` instance, or an instance of the `resource_class`, if provided
        :raises RepositoryError: if it cannot instantiate an instance of the resource class
        """
        if resource_class is None:
            resource_class = RepositoryResource

        if path.startswith(self.endpoint.url):
            path = path[len(str(self.endpoint.url)):]
        elif self.endpoint.external_url is not None and path.startswith(self.endpoint.external_url):
            path = path[len(str(self.endpoint.external_url)):]
        else:
            path = path

        try:
            return resource_class(repo=self, path=path)
        except TypeError as e:
            raise RepositoryError(f'Cannot get "{path}" as type "{resource_class.__name__}": {e}"') from e

    def __getitem__(self, item: str | slice) -> ResourceType:
        """Syntactic sugar for the `get_resource method`. It accepts either a string or a slice.

        If a string is used, it is used as the path, and the resource class defaults to
        `RepositoryResource`.

        If a slice is used, the "start" segment is used as the path and the "stop" segment
        is used as the resource class. If the "stop" segment is None, the resource class
        defaults to `RepositoryResource`.

        Examples:

            # get resource at "/foo" as a RepositoryResource object
            r = repo['/foo']
            instanceof(r, RepositoryResource)  # True

            # get resource at "/bar" as an OtherResource object
            r = repo['/bar':OtherResource]
            instanceof(r, OtherResource)  # True

        :raises TypeError: if non-string, non-slice key is used"""
        if isinstance(item, str):
            path = item
            resource_class = RepositoryResource
        elif isinstance(item, slice):
            path = item.start
            resource_class = item.stop if item.stop is not None else RepositoryResource
        else:
            raise TypeError(f'Cannot use a key of type "{type(item).__name__}" here')
        return self.get_resource(path, resource_class=resource_class)

    @contextmanager
    def transaction(self, keep_alive: int = 90):
        try:
            with self.client.transaction(keep_alive) as txn_client:
                self._txn_client = txn_client
                yield self._txn_client
        finally:
            # always clear the transaction client; otherwise the client
            # will raise an exception the next time it tries to create
            # a transaction
            self._txn_client = None

    def create(self, resource_class: Type[ResourceType] = None, **kwargs) -> ResourceType:
        resource_uri = self.client.create(**kwargs)
        return self.get_resource(resource_uri.uri, resource_class=resource_class).read()


class RepositoryResource:
    """An [LDP Resource](https://www.w3.org/TR/ldp/#ldpr) within a repository."""

    def __init__(self, repo: Repository, path: str = None):
        self.repo = repo
        self.path = path
        self._types: Optional[set[URLObject]] = None
        self._description_url: Optional[URLObject] = None
        self._graph: TrackChangesGraph = TrackChangesGraph()
        self._headers = None

    def __str__(self):
        return self.path if self.path is not None else '[NEW]'

    T = TypeVar('T', bound='RepositoryResource')

    def convert_to(self, cls: Type[T]) -> T:
        try:
            return cls(repo=self.repo, path=self.path)
        except TypeError as e:
            raise RepositoryError(f'Unable to convert {self.__class__.__name__} to {cls.__name__}: {e}')

    @property
    def url(self) -> Optional[URLObject]:
        if self.path is not None:
            return self.repo.endpoint.url.add_path(self.path.lstrip('/'))
        else:
            return None

    @property
    def description_url(self) -> Optional[URLObject]:
        return self._description_url

    @property
    def client(self) -> Client:
        return self.repo.client

    @property
    def graph(self) -> TrackChangesGraph:
        return self._graph

    @property
    def exists(self) -> bool:
        return self.url is not None and self._head().ok

    @property
    def is_gone(self) -> bool:
        return self.url is not None and self._head().status_code == HTTPStatus.GONE

    @property
    def is_binary(self) -> bool:
        if self._types is None:
            raise RepositoryError('Resource types unknown')
        return 'http://www.w3.org/ns/ldp#NonRDFSource' in self._types

    @property
    def headers(self):
        return self._headers

    def _head(self) -> Response:
        if self.url is None:
            raise RepositoryError('Resource has no URL')
        response = self.client.head(self.url)
        self._headers = response.headers
        self._types = {URLObject(link['url']) for link in response.links.values() if link['rel'] == 'type'}
        if 'describedby' in response.links:
            self._description_url = URLObject(response.links['describedby']['url'])
        return response

    def describe(self, model: Type[RDFResourceType]) -> RDFResourceType:
        return model(uri=URIRef(self.url), graph=self._graph)

    def attach_description(self, description: RDFResourceBase):
        description.uri = URIRef(self.url)
        self._graph = description.graph

    def get_resource(self, path: str, resource_class: Type[ResourceType]) -> ResourceType:
        url = self.url.add_path(path)
        return self.repo[url:resource_class]

    def read(self):
        head_response = self._head()
        if not head_response.ok:
            raise RepositoryError(f'Unable to read {self.url}', response=head_response)
        try:
            request_url = self.description_url or self.url
            text = self.client.get_description(request_url)

            self._graph = TrackChangesGraph().parse(data=text.value, format=text.media_type)

            # as a convenience, return itself; allows r = RepositoryResource(...).read() constructions
            return self
        except ClientError as e:
            raise RepositoryError(f'Unable to read {self.url}', response=e.response) from e

    def update(self):
        if not self._graph.has_changes:
            logger.debug(f'No changes for {self.url}')
            return
        logger.info(f'Sending update for {self.url}')
        request_url = self.description_url or self.url
        try:
            response = self.client.patch_graph(request_url, self.graph.deletes, self.graph.inserts)
            if not response.ok:
                raise RepositoryError(f'Unable to update {self.url}: {response}')
        except ClientError as e:
            raise RepositoryError(f'Unable to update {self.url}: {e}') from e
        else:
            self._graph.apply_changes()

    def delete(self):
        if not self.exists:
            logger.info(f'Resource {self.url} does not exist or is already deleted')
            return
        try:
            response = self.client.delete(self.url)
            if response.ok:
                logger.info(f'Deleted resource {self.url}')
            else:
                raise RepositoryError(f'Unable to delete {self.url}: {response}')
        except ClientError as e:
            raise RepositoryError(f'Unable to delete {self.url}: {e}') from e

    def walk(
        self,
        traverse: list[URIRef] = None,
        max_depth: int = inf,
        min_depth: int = -1,
        include_tombstones: bool = False,
        _current_depth: int = 0,
    ) -> Iterator[Union['RepositoryResource', 'Tombstone']]:
        if min_depth > max_depth:
            raise ValueError(f'min_depth ({min_depth}) cannot be greater than max_depth ({max_depth})')

        if traverse is None:
            # default to walking the ldp:contains relationships
            traverse = [ldp.contains]

        if _current_depth > min_depth:
            if self.is_gone and include_tombstones:
                yield Tombstone(self)
            elif self.exists:
                yield self.read()
            else:
                logger.error(f'{self.url} (or its tombstone) not found')
                return

        if traverse and _current_depth < max_depth:
            for _, p, o in self.graph.triples((URIRef(self.url), None, None)):
                if p in traverse:
                    yield from self.repo[str(o)].walk(
                        traverse=traverse,
                        max_depth=max_depth,
                        _current_depth=_current_depth + 1,
                    )


class Tombstone:
    def __init__(self, resource: RepositoryResource):
        self._resource = resource

    @property
    def url(self) -> Optional[URLObject]:
        return self._resource.url

    @property
    def path(self) -> Optional[str]:
        return self._resource.path


class ContainerResource(RepositoryResource):
    """An [LDP Container](https://www.w3.org/TR/ldp/#ldpc) resource."""

    def create_child(
            self,
            resource_class: Type[ResourceType] = RepositoryResource,
            description: RDFResourceType = None,
            **kwargs,
    ) -> ResourceType:
        if description is not None:
            # To create the resource and metadata at the same time (when the URI is not known),
            # we must use the empty URI for the subject, and serialize as Turtle. Fedora will
            # interpret the empty URI as a placeholder for "this resource".
            description.uri = URIRef('')
            resource = self.repo.create(
                resource_class=resource_class,
                container_path=self.path,
                headers={
                    'Content-Type': 'text/turtle',
                },
                data=description.graph.serialize(format='text/turtle').encode(),
                **kwargs,
            )
            # update the description URI the newly created URL
            description.uri = URIRef(resource.url)
        else:
            resource = self.repo.create(resource_class=resource_class, container_path=self.path, **kwargs)
        return resource


class BinaryResource(RepositoryResource):
    """An [LDP Non-RDF Source](https://www.w3.org/TR/ldp/#ldpnr) resource."""
    @property
    def size(self) -> int:
        """Size of the resource in bytes, as reported by the HTTP `Content-Length` header."""
        return int(self._headers['Content-Length'])

    @contextmanager
    def open(self):
        """Request the resource, and return a `BytesIO` object of its content."""
        response = self.client.get(self.url, stream=True)
        if not response.ok:
            raise RepositoryError(response)

        yield BytesIO(response.content)


class RepositoryError(Exception):
    def __init__(self, *args, response: Response = None):
        super().__init__(*args)
        self.response = response
        """HTTP response that triggered this error."""


class DataReadError(Exception):
    pass
