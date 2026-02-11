import dataclasses
import re
from argparse import Namespace
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.metadata import version
from string import Formatter
from typing import Any, Optional

import pysolr

from plastron.client import Endpoint, Client
from plastron.client.auth import get_authenticator
from plastron.handles import HandleServiceClient
from plastron.messaging.broker import Broker, ServerTuple
from plastron.models.fedora import FedoraResource
from plastron.repo import Repository, RepositoryResource, RepositoryError

UUID_REGEX = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)


def get_uuid_from_uri(uri: str) -> Optional[str]:
    if m := UUID_REGEX.search(uri):
        return m[1]
    else:
        return None


@dataclass
class PlastronContext:
    config: dict[str, Any] = None
    args: Namespace = None
    _repo: Repository = None
    _endpoint: Endpoint = None
    _client: Client = None
    _broker: Broker = None
    _solr: pysolr.Solr = None
    _handle_client: HandleServiceClient = None

    @property
    def version(self):
        return version('plastron-repo')

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
                    ua_string=f'plastron/{self.version}',
                    on_behalf_of=self.args.delegated_user,
                )
            except KeyError as e:
                raise RuntimeError(f"Missing configuration key {e} in section 'REPOSITORY'")

        return self._client

    @property
    def repo(self) -> Repository:
        if self._repo is None:
            self._repo = Repository(client=self.client)
        return self._repo

    @contextmanager
    def repo_configuration(self, delegated_user: str = None, ua_string: str = None) -> 'PlastronContext':
        if self.args is not None:
            args = Namespace(**{**self.args.__dict__, 'delegated_user': delegated_user, 'ua_string': ua_string})
        else:
            args = Namespace(delegated_user=delegated_user, ua_string=ua_string)
        yield dataclasses.replace(self, args=args)

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

    def get_public_url(self, resource: RepositoryResource) -> str:
        """Given a `RepositoryResource`, use the configuration value `PUBLICATION_WORKFLOW.PUBLIC_URL_PATTERN`
        to generate a URL. The pattern may use the Python formatting string syntax to insert
        resource-specific values into the string. Available fields are:

        * `path`: the full repository path to the resource
        * `container_path`: the repository path to the resource's parent container
        * `relpath`: same as `container_path`, but omits the leading "/"
        * `uuid`: the UUID portion of the resource's path
        * `iiif_id`: the repository path, with slashes replaced by colons and prefixed with "fcrepo"

        If one of these fields is requested by the pattern, but the context is unable to get a value
        for that field, raises a `RuntimeError`. If there is not a public URL pattern config value,
        raises a `RuntimeError`."""
        try:
            public_url_pattern = self.config.get('PUBLICATION_WORKFLOW', {})['PUBLIC_URL_PATTERN']
        except KeyError as e:
            raise RuntimeError(f"Missing configuration key {e} in section 'PUBLICATION_WORKFLOW'")

        formatter = Formatter()
        field_names = {name for _, name, *_ in formatter.parse(public_url_pattern) if name != ''}
        data = {}

        if 'path' in field_names:
            data['path'] = self.endpoint.repo_path(resource.url)

        if 'container_path' in field_names or 'relpath' in field_names:
            try:
                data['container_path'] = self.endpoint.repo_path(resource.describe(FedoraResource).parent.value)
                data['relpath'] = data['container_path'].lstrip('/')
            except RepositoryError as e:
                raise RuntimeError(f'Unable to retrieve container path for {resource.url}') from e

        if 'uuid' in field_names:
            uuid = get_uuid_from_uri(resource.url)
            if uuid is None:
                raise RuntimeError(f'Cannot create public URL; unable to find UUID in {resource.url}')
            data['uuid'] = uuid.lower()

        if 'iiif_id' in field_names:
            data['iiif_id'] = 'fcrepo' + self.endpoint.repo_path(resource.url).replace('/', ':')

        return public_url_pattern.format(**data)
