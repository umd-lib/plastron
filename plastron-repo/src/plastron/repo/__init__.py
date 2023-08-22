import logging
import shutil
import sys
from tempfile import NamedTemporaryFile
from typing import Optional, Type, Dict
from uuid import uuid4

import yaml
from rdflib import URIRef, Graph
from requests.auth import AuthBase
from urlobject import URLObject

from plastron.client import Client, Endpoint, ClientError, TransactionClient, RepositoryStructure
from plastron.client.auth import get_authenticator
from plastron.utils import ItemLog
from plastron.rdfmapping.resources import RDFResource, RDFResourceBase

logger = logging.getLogger(__name__)


def mint_fragment_identifier() -> str:
    return str(uuid4())


def get_structure(structure_name: Optional[str]) -> RepositoryStructure:
    if structure_name is None:
        return RepositoryStructure.FLAT
    return RepositoryStructure[structure_name.upper()]


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
        self.client = client
        self.endpoint = client.endpoint

    def __getitem__(self, item: str):
        if item.startswith(self.endpoint.url):
            path = item[len(self.endpoint.url):]
        else:
            path = item
        if '#' in path:
            # fragment resource
            parent_path, hash_id = path.split('#', 1)
            return FragmentResource(parent=RepositoryResource(repo=self, path=parent_path), identifier=hash_id)
        # regular resource
        return RepositoryResource(repo=self, path=path)


class DescribableResource:
    def __init__(self):
        self._graph: Optional[Graph] = None
        self._description: Optional[RDFResourceBase] = None
        self._model: Optional[Type[RDFResourceBase]] = None

    @property
    def url(self):
        raise NotImplementedError

    def read(self):
        """
        Implementations of this method are responsible for populating
        the :attr:`_graph` attribute.
        """
        raise NotImplementedError

    @property
    def graph(self) -> Graph:
        if self._graph is None:
            self.read()
        return self._graph

    @property
    def description(self) -> RDFResourceBase:
        return self._description

    @property
    def model(self) -> Type[RDFResourceBase]:
        return self._model

    @model.setter
    def model(self, value: Type[RDFResourceBase]):
        self._model = value
        self._description = self.model(uri=URIRef(self.url), graph=self.graph)

    def describe_as(self, model: Type[RDFResource]):
        return model(uri=URIRef(self.url), graph=self.graph)


class RepositoryResource(DescribableResource):
    def __init__(self, repo: Repository, path: str = None):
        super().__init__()
        self.repo = repo
        self.path = path
        self.client = repo.client
        self._types = None
        self._fragments = None

    @property
    def url(self) -> Optional[URLObject]:
        if self.path is not None:
            return self.repo.endpoint.url.add_path(self.path.lstrip('/'))
        else:
            return None

    def exists(self) -> bool:
        return self.url is not None and self.client.head(self.url).ok

    def read(self, model: Optional[Type[RDFResourceBase]] = None):
        if self.url is None:
            raise RepositoryError('Resource has no URL')
        response = self.client.head(self.url)
        self._types = [link['url'] for link in response.links.values() if link['rel'] == 'type']
        try:
            # TODO: different handling for binaries?
            _, self._graph = self.client.get_graph(self.url)
        except ClientError as e:
            raise RepositoryError(f'Unable to retrieve {self.url}: {e}') from e
        self.model = model or RDFResourceBase
        subjects = {URLObject(s) for s in set(self.graph.subjects())}
        self._fragments = [
            FragmentResource(identifier=url.fragment, parent=self)
            for url in filter(lambda s: s.startswith(f'{self.url}#'), subjects)
        ]

    def save(self):
        pass

    @property
    def is_binary(self):
        if self._types is None:
            raise RepositoryError('Resource types unknown')
        return 'http://www.w3.org/ns/ldp#NonRDFSource' in self._types

    def fragments(self):
        subjects = {URLObject(s) for s in set(self.graph.subjects())}
        for url in filter(lambda s: s.startswith(f'{self.url}#'), subjects):
            yield FragmentResource(identifier=url.fragment, parent=self)


class FragmentResource(DescribableResource):
    def __init__(self, parent: RepositoryResource = None, identifier: str = None):
        super().__init__()
        self.parent = parent
        if identifier is not None:
            self.identifier = identifier
        else:
            self.identifier = mint_fragment_identifier()
        self._graph = None

    @property
    def url(self) -> URLObject:
        if self.parent:
            return self.parent.url.with_fragment(self.identifier)
        else:
            return URLObject(f'#{self.identifier}')

    def exists(self) -> bool:
        return self.parent.exists()

    def read(self, model: Optional[Type[RDFResourceBase]] = None):
        self._graph = Graph()
        for triple in self.parent.graph.triples((URIRef(self.url), None, None)):
            self._graph.add(triple)
        self.model = model or RDFResourceBase


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
