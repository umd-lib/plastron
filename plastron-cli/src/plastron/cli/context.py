import re
from argparse import Namespace
from dataclasses import dataclass
from importlib.metadata import version
from typing import Dict, Any, Optional

import pysolr

from plastron.client import Endpoint, Client, RepositoryStructure
from plastron.client.auth import get_authenticator
from plastron.handles import HandleServiceClient
from plastron.repo import Repository
from plastron.stomp.broker import Broker, ServerTuple


@dataclass
class PlastronContext:
    config: Dict[str, Any] = None
    args: Namespace = None
    _repo: Repository = None
    _endpoint: Endpoint = None
    _client: Client = None
    _broker: Broker = None
    _solr: pysolr.Solr = None
    _handle_client: HandleServiceClient = None

    @property
    def version(self):
        return version('plastron-cli')

    @property
    def endpoint(self) -> Endpoint:
        if self._endpoint is None:
            repo_config = self.config.get('REPOSITORY', {})
            try:
                self._endpoint = Endpoint(
                    url=repo_config['REST_ENDPOINT'],
                    default_path=repo_config.get('RELPATH', '/'),
                    external_url=repo_config.get('REPO_EXTERNAL_URL'),
                )
            except KeyError as e:
                raise RuntimeError(f"Missing configuration key {e} in section 'REPOSITORY'")

        return self._endpoint

    @property
    def client(self) -> Client:
        if self._client is None:
            repo_config = self.config.get('REPOSITORY', {})
            try:
                # TODO: respect the batch mode flag when getting the authenticator
                self._client = Client(
                    endpoint=self.endpoint,
                    auth=get_authenticator(repo_config),
                    ua_string=f'plastron/{version}',
                    on_behalf_of=self.args.delegated_user,
                    structure=RepositoryStructure[repo_config.get('STRUCTURE', 'flat').upper()]
                )
            except KeyError as e:
                raise RuntimeError(f"Missing configuration key {e} in section 'REPOSITORY'")

        return self._client

    @property
    def repo(self) -> Repository:
        if self._repo is None:
            self._repo = Repository(client=self.client)
        return self._repo

    @property
    def broker(self) -> Broker:
        if self._broker is None:
            broker_config = self.config.get('MESSAGE_BROKER', {})
            try:
                self._broker = Broker(
                    server=ServerTuple.from_string(broker_config['SERVER']),
                    message_store_dir=broker_config['MESSAGE_STORE_DIR'],
                    destinations=broker_config['DESTINATIONS'],
                )
            except KeyError as e:
                raise RuntimeError(f"Missing configuration key {e} in section 'MESSAGE_BROKER'")

        return self._broker

    @property
    def solr(self) -> pysolr.Solr:
        if self._solr is None:
            solr_config = self.config.get('SOLR', {})
            try:
                self._solr = pysolr.Solr(solr_config['URL'], always_commit=True, timeout=10)
            except KeyError as e:
                raise RuntimeError(f"Missing configuration key {e} in section 'SOLR'")

        return self._solr

    @property
    def handle_client(self) -> HandleServiceClient:
        if self._handle_client is None:
            # try to instantiate a handle client
            config = self.config.get('PUBLICATION_WORKFLOW', {})
            try:
                self._handle_client = HandleServiceClient(
                    endpoint_url=config['HANDLE_ENDPOINT'],
                    jwt_token=config['HANDLE_JWT_TOKEN'],
                    default_prefix=config['HANDLE_PREFIX'],
                    default_repo=config['HANDLE_REPO'],
                )
            except KeyError as e:
                raise RuntimeError(f"Missing configuration key {e} in section 'PUBLICATION_WORKFLOW'")

        return self._handle_client

    def get_public_url(self, repo_uri: str) -> str:
        try:
            public_url_pattern = self.config.get('PUBLICATION_WORKFLOW', {})['PUBLIC_URL_PATTERN']
        except KeyError as e:
            raise RuntimeError(f"Missing configuration key {e} in section 'PUBLICATION_WORKFLOW'")

        uuid = get_uuid_from_uri(repo_uri)
        if uuid is None:
            raise RuntimeError(f'Cannot create public URL; unable to find UUID in {repo_uri}')

        return public_url_pattern.format(uuid=uuid.lower())


UUID_REGEX = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)


def get_uuid_from_uri(uri: str) -> Optional[str]:
    if m := UUID_REGEX.search(uri):
        return m[1]
    else:
        return None
