import dataclasses
import logging
from dataclasses import dataclass
from typing import Any

from requests import Session
from requests_jwtauth import HTTPBearerAuth

from plastron.namespaces import dcterms, umdtype
from plastron.rdfmapping.descriptors import DataProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.rules import is_handle

logger = logging.getLogger(__name__)


def parse_handle_string(handle: str) -> list[str]:
    if handle.startswith('hdl:'):
        handle = handle[4:]
    try:
        return handle.split('/', 1)
    except ValueError as e:
        raise HandleError(
            'Handle must be a string in the form "{prefix}/{suffix}" or "hdl:{prefix}/{suffix}'
        ) from e


def parse_result(result: dict[str, Any]) -> dict[str, Any]:
    logger.debug(f'Raw result: {result}')
    if 'request' in result:
        request = result['request']
        del result['request']
        result.update(request)
    return result


@dataclass
class HandleInfo:
    exists: bool
    handle_url: str = None
    prefix: str = None
    suffix: str = None
    url: str = None
    repo: str = None
    repo_id: str = None

    def __str__(self):
        """The handle in `{prefix}/{suffix}` form"""
        return f'{self.prefix}/{self.suffix}'

    @property
    def hdl_uri(self):
        """The handle in `hdl:{prefix}/{suffix}` form"""
        return f'hdl:{self}'


class HandleServiceClient:
    def __init__(self, endpoint_url: str, jwt_token: str, default_prefix: str = None, default_repo: str = None):
        self.endpoint_url = endpoint_url
        self.default_prefix = default_prefix
        self.default_repo = default_repo
        self.session = Session()
        self.session.auth = HTTPBearerAuth(jwt_token)

    def get_info(self, prefix: str, suffix: str):
        url = self.endpoint_url + '/handles/info'
        response = self.session.get(
            url=url,
            params={
                'prefix': prefix,
                'suffix': suffix,
            },
        )
        if not response.ok:
            raise HandleServerError(str(response))

        return HandleInfo(**parse_result(response.json()))

    def find_handle(self, repo_id: str, repo: str = None) -> HandleInfo:
        url = self.endpoint_url + '/handles/exists'
        response = self.session.get(
            url=url,
            params={
                'repo': repo or self.default_repo,
                'repo_id': repo_id,
            },
        )
        if not response.ok:
            raise HandleServerError(str(response))

        return HandleInfo(**parse_result(response.json()))

    def create_handle(self, repo_id: str, url: str, prefix: str = None, repo: str = None) -> HandleInfo:
        request = {
            'prefix': prefix or self.default_prefix,
            'repo': repo or self.default_repo,
            'repo_id': repo_id,
            'url': url,
        }
        response = self.session.post(
            f'{self.endpoint_url}/handles',
            json=request,
        )
        if not response.ok:
            raise HandleServerError(str(response))

        return HandleInfo(exists=True, **parse_result(response.json()))

    def update_handle(self, handle_info: HandleInfo, **fields) -> HandleInfo:
        updated_handle_info = dataclasses.replace(handle_info, **fields)
        response = self.session.patch(
            f'{self.endpoint_url}/handles/{handle_info.prefix}/{handle_info.suffix}',
            json=dataclasses.asdict(updated_handle_info),
        )
        if not response.ok:
            raise HandleServerError(str(response))

        return updated_handle_info


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
