import dataclasses
import logging
from dataclasses import dataclass
from http import HTTPStatus
from typing import Optional, List

import requests
from requests_jwtauth import HTTPBearerAuth

from plastron.namespaces import dcterms, umdtype
from plastron.rdfmapping.descriptors import DataProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.rules import is_handle

logger = logging.getLogger(__name__)


def parse_handle_string(handle: str) -> List[str]:
    if handle.startswith('hdl:'):
        handle = handle[4:]
    try:
        return handle.split('/', 1)
    except ValueError as e:
        raise HandleError(
            'Handle must be a string in the form "{prefix}/{suffix}" or "hdl:{prefix}/{suffix}'
        ) from e


@dataclass
class Handle:
    prefix: str
    suffix: str
    url: str = None

    @classmethod
    def parse(cls, handle_string: str) -> 'Handle':
        prefix, suffix = parse_handle_string(handle_string)
        return cls(prefix=prefix, suffix=suffix)

    def __str__(self):
        return '/'.join((self.prefix, self.suffix))

    @property
    def hdl_uri(self):
        """The handle in `hdl:{prefix}/{suffix}` form"""
        return f'hdl:{self}'

    @property
    def has_url(self):
        return self.url is not None


class HandleServiceClient:
    def __init__(self, endpoint_url: str, jwt_token: str, default_prefix: str = None, default_repo: str = None):
        self.endpoint_url = endpoint_url
        self.auth = HTTPBearerAuth(jwt_token)
        self.default_prefix = default_prefix
        self.default_repo = default_repo

    def resolve(self, handle: Handle) -> Optional[Handle]:
        """Attempt to resolve the given handle. If successful, returns a new `Handle`
        object with the `url` field populated. If the handle is not found, returns
        `None`. For any other error response, raises a `HandleServerError`."""
        path = f'/handles/{handle.prefix}/{handle.suffix}'
        url = self.endpoint_url + path
        response = requests.get(url=url, auth=self.auth)
        if response.status_code == HTTPStatus.NOT_FOUND:
            return None
        elif not response.ok:
            raise HandleServerError(str(response))
        return Handle(
            prefix=handle.prefix,
            suffix=handle.suffix,
            url=response.json().get('url', None),
        )

    def find_handle(self, repo_uri: str, repo: str = None) -> Optional[Handle]:
        url = self.endpoint_url + '/handles/exists'
        response = requests.get(
            url=url,
            auth=self.auth,
            params={
                'repo': repo or self.default_repo,
                'repo_id': repo_uri,
            },
        )
        if not response.ok:
            raise HandleServerError(str(response))

        result = response.json()
        logger.debug(result)

        if not result['exists']:
            return None

        return Handle(
            prefix=result['prefix'],
            suffix=result['suffix'],
            url=result['url'],
        )

    def create_handle(self, repo_uri: str, url: str, prefix: str = None, repo: str = None) -> Handle:
        request = {
            'prefix': prefix or self.default_prefix,
            'repo': repo or self.default_repo,
            'repo_id': repo_uri,
            'url': url,
        }
        response = requests.post(
            f'{self.endpoint_url}/handles',
            json=request,
            auth=self.auth,
        )
        if not response.ok:
            raise HandleServerError(str(response))
        result = response.json()
        logger.debug(result)
        return Handle(
            prefix=result['request']['prefix'],
            suffix=result['suffix'],
            url=result['request']['url'],
        )

    def update_handle(self, handle: Handle, **fields) -> Handle:
        updated_handle = dataclasses.replace(handle, **fields)
        response = requests.patch(
            f'{self.endpoint_url}/handles/{handle.prefix}/{handle.suffix}',
            json=dataclasses.asdict(updated_handle),
            auth=self.auth,
        )
        if not response.ok:
            raise HandleServerError(str(response))
        result = response.json()
        logger.debug(result)
        return updated_handle


class HandleError(Exception):
    pass


class HandleServerError(HandleError):
    pass


class HandleNotFoundError(HandleServerError):
    def __init__(self, handle, *args):
        super().__init__(*args)
        self.handle = handle


class HandleBearingResource(RDFResource):
    """This class be used by itself for instances where the handle field is the only
    one needed, or it can be used as a mix-in to other full models to give them a handle
    field."""
    handle = DataProperty(dcterms.identifier, datatype=umdtype.handle, validate=is_handle)

    @property
    def has_handle(self) -> bool:
        """Convenience property for whether this object has a valid handle."""
        return bool(len(self.handle) > 0 and self.handle.is_valid)
