import dataclasses
import logging
from dataclasses import dataclass
from typing import Optional

import requests
from requests_jwtauth import HTTPBearerAuth

from plastron.namespaces import dcterms, umdtype
from plastron.rdfmapping.descriptors import DataProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.rules import is_handle

logger = logging.getLogger(__name__)


@dataclass
class Handle:
    prefix: str
    suffix: str
    url: str

    def __str__(self):
        return '/'.join((self.prefix, self.suffix))

    @property
    def hdl_uri(self):
        """The handle in `hdl:{prefix}/{suffix}` form"""
        return f'hdl:{self}'


class HandleServiceClient:
    def __init__(self, endpoint_url: str, jwt_token: str, default_prefix: str = None, default_repo: str = None):
        self.endpoint_url = endpoint_url
        self.auth = HTTPBearerAuth(jwt_token)
        self.default_prefix = default_prefix
        self.default_repo = default_repo

    def get_handle(self, repo_uri: str, repo: str = None) -> Optional[Handle]:
        response = requests.get(
            f'{self.endpoint_url}/handles/exists',
            params={
                'repo': repo or self.default_repo,
                'repo_id': repo_uri,
            },
            auth=self.auth,
        )
        if not response.ok:
            raise HandleServerError(str(response))
        result = response.json()
        logger.debug(result)
        if result['exists']:
            return Handle(
                prefix=result['prefix'],
                suffix=result['suffix'],
                url=result['url'],
            )
        else:
            return None

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


class HandleServerError(Exception):
    pass


class HandleBearingResource(RDFResource):
    """This class be used by itself for instances where the handle field is the only
    one need, or it can be used as a mix-in to other full models to give them a handle
    field."""
    handle = DataProperty(dcterms.identifier, datatype=umdtype.handle, validate=is_handle)
