import dataclasses
import re
from argparse import Namespace
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cached_property
from importlib.metadata import version
from string import Formatter
from typing import Any, Optional

import pysolr

from plastron.client import Endpoint, Client
from plastron.client.auth import get_authenticator
from plastron.client.proxied import ProxiedClient
from plastron.handles import HandleServiceClient
from plastron.messaging.broker import Broker, ServerTuple, HeartbeatTuple
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

    @property
    def version(self):
        return version('plastron-repo')

    @cached_property
    def endpoint(self) -> Endpoint:
        repo_config = self.config.get('REPOSITORY', {})
        try:
            return Endpoint(
                url=repo_config['REST_ENDPOINT'],
                default_path=repo_config.get('RELPATH', '/'),
            )
        except KeyError as e:
            raise RuntimeError(f"Missing configuration key {e} in section 'REPOSITORY'")

    @cached_property
    def client(self) -> Client:
        delegated_user = self.args.delegated_user if hasattr(self.args, 'delegated_user') else None
        repo_config = self.config.get('REPOSITORY', {})
        authenticator = get_authenticator(repo_config)
        try:
            if 'ORIGIN' in repo_config:
                return ProxiedClient(
                    endpoint=self.endpoint,
                    origin_endpoint=Endpoint(url=repo_config['ORIGIN']),
                    auth=authenticator,
                    ua_string=f'plastron/{self.version}',
                    on_behalf_of=delegated_user,
                )
            else:
                return Client(
                    endpoint=self.endpoint,
                    auth=authenticator,
                    ua_string=f'plastron/{self.version}',
                    on_behalf_of=delegated_user,
                )
        except KeyError as e:
            raise RuntimeError(f"Missing configuration key {e} in section 'REPOSITORY'")

    @cached_property
    def repo(self) -> Repository:
        return Repository(client=self.client)

    @contextmanager
    def repo_configuration(self, delegated_user: str = None, ua_string: str = None) -> Generator['PlastronContext']:
        if self.args is not None:
            args = Namespace(**{**self.args.__dict__, 'delegated_user': delegated_user, 'ua_string': ua_string})
        else:
            args = Namespace(delegated_user=delegated_user, ua_string=ua_string)
        yield dataclasses.replace(self, args=args)

    @cached_property
    def broker(self) -> Broker:
        broker_config = self.config.get('MESSAGE_BROKER', {})
        heartbeat_intervals = broker_config.get('HEARTBEAT')
        if heartbeat_intervals is not None:
            heartbeat = HeartbeatTuple.from_dict(heartbeat_intervals)
        else:
            heartbeat = None
        try:
            return Broker(
                server=ServerTuple.from_string(broker_config['SERVER']),
                message_store_dir=broker_config['MESSAGE_STORE_DIR'],
                destinations=broker_config['DESTINATIONS'],
                heartbeat=heartbeat,
            )
        except KeyError as e:
            raise RuntimeError(f"Missing configuration key {e} in section 'MESSAGE_BROKER'")

    @cached_property
    def solr(self) -> pysolr.Solr:
        solr_config = self.config.get('SOLR', {})
        try:
            return pysolr.Solr(solr_config['URL'], always_commit=True, timeout=10)
        except KeyError as e:
            raise RuntimeError(f"Missing configuration key {e} in section 'SOLR'")

    @cached_property
    def handle_client(self) -> HandleServiceClient:
        # try to instantiate a handle client
        config = self.config.get('PUBLICATION_WORKFLOW', {})
        try:
            return HandleServiceClient(
                endpoint_url=config['HANDLE_ENDPOINT'],
                jwt_token=config['HANDLE_JWT_TOKEN'],
                default_prefix=config['HANDLE_PREFIX'],
                default_repo=config['HANDLE_REPO'],
            )
        except KeyError as e:
            raise RuntimeError(f"Missing configuration key {e} in section 'PUBLICATION_WORKFLOW'")

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
