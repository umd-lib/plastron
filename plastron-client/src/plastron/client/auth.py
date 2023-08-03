from typing import Mapping, Any, Optional

from requests import PreparedRequest
from requests.auth import AuthBase, HTTPBasicAuth
from requests_jwtauth import HTTPBearerAuth, JWTSecretAuth


class ClientCertAuth(AuthBase):
    def __init__(self, cert: str, key: str):
        self.cert = cert
        self.key = key

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.cert = (self.cert, self.key)
        return request


def get_authenticator(config: Mapping[str, Any]) -> Optional[AuthBase]:
    if 'AUTH_TOKEN' in config:
        return HTTPBearerAuth(token=config['AUTH_TOKEN'])
    elif 'JWT_SECRET' in config:
        return JWTSecretAuth(
            secret=config['JWT_SECRET'],
            claims={
                'sub': 'plastron',
                'iss': 'plastron',
                'role': 'fedoraAdmin'
            }
        )
    elif 'CLIENT_CERT' in config and 'CLIENT_KEY' in config:
        return ClientCertAuth(
            cert=config['CLIENT_CERT'],
            key=config['CLIENT_KEY'],
        )
    elif 'FEDORA_USER' in config and 'FEDORA_PASSWORD' in config:
        return HTTPBasicAuth(
            username=config['FEDORA_USER'],
            password=config['FEDORA_PASSWORD'],
        )
    else:
        return None
