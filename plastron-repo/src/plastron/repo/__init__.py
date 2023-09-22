import logging
import shutil
import sys
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import Optional, Type, Dict, Union, TypeVar, Set, List, Iterator
from uuid import uuid4

import yaml
from math import inf
from rdflib import URIRef, Namespace
from requests import Response
from requests.auth import AuthBase
from urlobject import URLObject

from plastron.client import Client, Endpoint, ClientError, TransactionClient, RepositoryStructure
from plastron.client.auth import get_authenticator
from plastron.rdfmapping.graph import TrackChangesGraph
from plastron.rdfmapping.resources import RDFResourceBase, RDFResourceType
from plastron.utils import ItemLog

logger = logging.getLogger(__name__)
ldp = Namespace('http://www.w3.org/ns/ldp#')


def mint_fragment_identifier() -> str:
    return str(uuid4())


def get_structure(structure_name: Optional[str]) -> RepositoryStructure:
    if structure_name is None:
        return RepositoryStructure.FLAT
    return RepositoryStructure[structure_name.upper()]


ResourceType = TypeVar('ResourceType', bound='RepositoryResource')


class Repository:
    @classmethod
    def from_config_file(cls, filename: str) -> 'Repository':
        with open(filename) as file:
            return cls.from_config(config=yaml.safe_load(file).get('REPOSITORY', {}))

    @classmethod
    def from_config(cls, config: Dict[str, str]) -> 'Repository':
        endpoint = Endpoint(
            url=config['REST_ENDPOINT'],
            default_path=config.get('RELPATH', '/'),
            external_url=config.get('REPO_EXTERNAL_URL', None)
        )
        client = Client(
            endpoint=endpoint,
            auth=get_authenticator(config),
            structure=get_structure(config.get('STRUCTURE', None)),
            server_cert=config.get('SERVER_CERT', None),
        )
        return cls(client=client)

    @classmethod
    def from_url(cls, url: str, auth: Optional[AuthBase] = None) -> 'Repository':
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

        By default, returns an object of type RepositoryResource, but you may pass a different
        resource class in the resource_class parameter. That class must support a constructor
        with keyword arguments "repo" and "path".

        :returns: RepositoryResource instance, or an instance of the resource_class, if provided
        :raises RepositoryError: if it cannot instantiate an instance of the resource class
        """
        if resource_class is None:
            resource_class = RepositoryResource
        if path.startswith(self.endpoint.url):
            path = path[len(self.endpoint.url):]
        else:
            path = path
        try:
            return resource_class(repo=self, path=path)
        except TypeError as e:
            raise RepositoryError(f'Cannot get "{path}" as type "{resource_class.__name__}": {e}"') from e

    def __getitem__(self, item: Union[str, slice]) -> ResourceType:
        """Syntactic sugar for the get_resource method. It accepts either a string or a slice.

        If a string is used, it is used as the path, and the resource class defaults to
        RepositoryResource.

        If a slice is used, the "start" segment is used as the path and the "stop" segment
        is used as the resource class. If it is None, defaults to RepositoryResource.

        Examples::

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
        with self.client.transaction(keep_alive) as txn_client:
            self._txn_client = txn_client
            yield self._txn_client
            self._txn_client = None

    def create(self, resource_class: Type[ResourceType] = None, **kwargs) -> ResourceType:
        resource_uri = self.client.create(**kwargs)
        return self.get_resource(resource_uri.uri, resource_class=resource_class)


class RepositoryResource:
    """A single HTTP/LDP resource within a repository."""

    def __init__(self, repo: Repository, path: str = None):
        self.repo = repo
        self.path = path
        self._types: Optional[Set[URLObject]] = None
        self._description_url: Optional[URLObject] = None
        self._graph: TrackChangesGraph = TrackChangesGraph()

    def __str__(self):
        return self.path if self.path is not None else '[NEW]'

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
    def is_binary(self) -> bool:
        if self._types is None:
            raise RepositoryError('Resource types unknown')
        return 'http://www.w3.org/ns/ldp#NonRDFSource' in self._types

    def _head(self) -> Response:
        if self.url is None:
            raise RepositoryError('Resource has no URL')
        response = self.client.head(self.url)
        self._types = {URLObject(link['url']) for link in response.links.values() if link['rel'] == 'type'}
        if 'describedby' in response.links:
            self._description_url = URLObject(response.links['describedby']['url'])
        return response

    def describe(self, model: Type[RDFResourceType]) -> RDFResourceType:
        return model(uri=URIRef(self.url), graph=self._graph)

    def attach_description(self, description: RDFResourceBase):
        description.uri = URIRef(self.url)
        self._graph = description.graph

    def read(self):
        self._head()
        try:
            # TODO: different handling for binaries?
            (uri, description_uri), self._graph = self.client.get_graph(self.url)
            self._description_url = URLObject(description_uri)
        except ClientError as e:
            raise RepositoryError(f'Unable to retrieve {self.url}: {e}') from e

    def save(self):
        self._graph.apply_changes()
        try:
            logger.debug(f'Putting graph to {self.url}')
            response = self.client.put_graph(self.url, self._graph)
            if not response.ok:
                raise RepositoryError(f'Unable to save {self.url}: {response}')
        except ClientError as e:
            raise RepositoryError(f'Unable to save {self.url}: {e}') from e

    def update(self):
        if not self._graph.has_changes:
            logger.debug(f'No changes for {self.url}')
            return
        logger.info(f'Sending update for {self.url}')
        sparql_update = self.repo.client.build_sparql_update(self._graph.deletes, self._graph.inserts)
        logger.debug(sparql_update)
        try:
            response = self.client.patch(
                self.url,
                headers={
                    'Content-Type': 'application/sparql-update'
                },
                data=sparql_update,
            )
            if not response.ok:
                raise RepositoryError(f'Unable to update {self.url}: {response}')
        except ClientError as e:
            raise RepositoryError(f'Unable to update {self.url}: {e}') from e

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
            traverse: List[URIRef] = None,
            max_depth: int = inf,
            min_depth: int = -1,
            _current_depth: int = 0,
    ) -> Iterator['RepositoryResource']:
        if min_depth > max_depth:
            raise ValueError(f'min_depth ({min_depth}) cannot be greater than max_depth ({max_depth})')
        if traverse is None:
            # default to walking the ldp:contains relationships
            traverse = [ldp.contains]
        if _current_depth > min_depth:
            self.read()
            yield self
        if traverse and _current_depth < max_depth:
            for _, p, o in self.graph.triples((URIRef(self.url), None, None)):
                if p in traverse:
                    yield from self.repo[str(o)].walk(
                        traverse=traverse,
                        max_depth=max_depth,
                        _current_depth=_current_depth + 1,
                    )


class ContainerResource(RepositoryResource):
    """An LDP container resource."""

    def create_child(
            self,
            resource_class: Type[ResourceType] = RepositoryResource,
            description: RDFResourceType = None,
            **kwargs,
    ) -> ResourceType:
        resource = self.repo.create(resource_class=resource_class, container_path=self.path, **kwargs)
        if description is not None:
            resource.attach_description(description)
        return resource


class BinaryResource(RepositoryResource):
    pass


class RepositoryError(Exception):
    pass


class ResourceList:
    def __init__(self, client: Client, uri_list=None, file=None, completed_file=None):
        self.client = client
        self.uri_list = uri_list
        self.file = file
        self.use_transaction = True
        if completed_file is not None:
            logger.info(f'Reading the completed items log from {completed_file}')
            # read the log of completed items
            fieldnames = ['uri', 'title', 'timestamp']
            try:
                self.completed = ItemLog(completed_file, fieldnames, 'uri')
                logger.info(f'Found {len(self.completed)} completed item(s)')
            except Exception as e:
                logger.error(f"Non-standard map file specified: {e}")
                raise
        else:
            self.completed = None
        self.completed_buffer = None

    def get_uris(self):
        if self.file is not None:
            if self.file == '-':
                # special filename "-" means STDIN
                for line in sys.stdin:
                    yield line
            else:
                with open(self.file) as fh:
                    for line in fh:
                        yield line.rstrip()
        else:
            for uri in self.uri_list:
                yield uri

    def get_resources(self, client: Client, traverse=None):
        repo = client.endpoint
        for uri in self.get_uris():
            if not repo.contains(uri):
                logger.warning(f'Resource {uri} is not contained within the repository {repo.url}')
                continue
            for resource, graph in client.recursive_get(uri, traverse=traverse):
                yield resource, graph

    def process(self, method, use_transaction=True, traverse=None):
        self.use_transaction = use_transaction
        if traverse is not None:
            predicate_list = ', '.join(p.n3() for p in traverse)
            logger.info(f"{method.__name__} will traverse the following predicates: {predicate_list}")

        if use_transaction:
            # set up a temporary ItemLog that will be copied to the real item log upon completion of the transaction
            self.completed_buffer = ItemLog(
                NamedTemporaryFile().name,
                ['uri', 'title', 'timestamp'],
                'uri',
                header=False
            )
            with self.client.transaction(keep_alive=90) as txn_client:  # type: TransactionClient
                for resource, graph in self.get_resources(client=txn_client, traverse=traverse):
                    try:
                        method(resource, graph)
                    except ClientError as e:
                        logger.error(f'{method.__name__} failed for {resource}: {e}: {e.response.text}')
                        # if anything fails while processing of the list of uris, attempt to
                        # roll back the transaction. Failures here will be caught by the main
                        # loop's exception handler and should trigger a system exit
                        try:
                            txn_client.rollback()
                            logger.warning('Transaction rolled back.')
                            return False
                        except ClientError:
                            logger.error('Unable to roll back transaction, aborting')
                            raise
                txn_client.commit()
                if self.completed and self.completed.filename:
                    shutil.copyfile(self.completed_buffer.filename, self.completed.filename)
                return True
        else:
            for resource, graph in self.get_resources(client=self.client, traverse=traverse):
                try:
                    method(resource, graph)
                except ClientError as e:
                    logger.error(f'{method.__name__} failed for {resource}: {e}: {e.response.text}')
                    logger.warning(f'Continuing {method.__name__} with next item')
            return True

    def log_completed(self, uri, title, timestamp):
        if self.completed is not None:
            row = {'uri': uri, 'title': title, 'timestamp': timestamp}
            if self.use_transaction:
                self.completed_buffer.writerow(row)
            else:
                self.completed.writerow(row)


class DataReadError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message
