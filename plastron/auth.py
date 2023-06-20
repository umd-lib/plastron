import time

from abc import ABC, abstractmethod
from jwcrypto.jwk import JWK  # type: ignore
from jwcrypto.jwt import JWT  # type: ignore
import logging
from requests import Session
from typing import Dict, List, Type
from .exceptions import ConfigError


class Auth(ABC):
    @abstractmethod
    def __init__(self, config: Dict[str, str]):
        pass

    @abstractmethod
    def configure_session(self, session: Session) -> None:
        '''Modifies given Session with appropriate values for auth method'''
        pass

    def is_expired(self) -> bool:
        '''
        Returns True if the authorization has expired, False otherwise.

        The default implementation always returns False.
        '''
        return False

    def refresh_auth(self, session: Session) -> None:
        '''
        Refreshes an expired authorization, and updates the given session.

        The default implementation does nothing.
        '''
        pass

    @classmethod
    def handles(cls, config: Dict[str, str]) -> bool:
        pass


class AuthFactory:
    @classmethod
    def create(cls, config: Dict[str, str]) -> Auth:
        if config is None:
            raise ValueError("config is empty.")

        # Auth subclasses to check, in priority order
        auth_classes: List[Type[Auth]] = [
            ProvidedJwtTokenAuth,
            JwtSecretAuth,
            ClientCertAuth,
            FedoraUserAuth
        ]

        for c in auth_classes:
            if c.handles(config):
                return c(config)

        raise ValueError("Could not create auth from given config.")


class ProvidedJwtTokenAuth(Auth):
    '''Auth for AUTH_TOKEN provided in config'''
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.logger = logging.getLogger(type(self).__name__)
        self.jwt_token = config['AUTH_TOKEN']
        self.batch_jwt_token = config.get('BATCH_MODE', {}).get('PLASTRON_BATCH', None)

    @classmethod
    def handles(cls, config: Dict[str, str]) -> bool:
        return 'AUTH_TOKEN' in config

    def configure_session(self, session: Session) -> None:
        if not session.batch_mode:
            self.logger.info('Using Default Mode')
            session.headers.update({'Authorization': f"Bearer {self.jwt_token}"})

        elif self.batch_jwt_token is None:
            raise ConfigError("Batch mode was set, but batch credentials were not configured properly")

        else:
            self.logger.info('Using Batch Mode')
            session.headers.update({'Authorization': f"Bearer {self.batch_jwt_token}"})

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}('
                f'jwt_token=<REDACTED>'
                f')')


class JwtSecretAuth(Auth):
    EXPIRATION_TIME_IN_SECONDS: int = 3600

    '''Auth for JWT_SECRET provided in config'''
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.logger = logging.getLogger(type(self).__name__)
        self.jwt_secret = config['JWT_SECRET']
        self.expiration_time = time.time() + JwtSecretAuth.EXPIRATION_TIME_IN_SECONDS
        self.jwt_token = self._create_auth_token(self.jwt_secret, self.expiration_time).serialize()

    @classmethod
    def handles(cls, config: Dict[str, str]) -> bool:
        return 'JWT_SECRET' in config

    def configure_session(self, session: Session) -> None:
        session.headers.update({'Authorization': f"Bearer {self.jwt_token}"})

    def is_expired(self) -> bool:
        return self.expiration_time < time.time()

    def refresh_auth(self, session: Session) -> None:
        # Refresh if within grace period of the expiration time
        grace_period_in_seconds = 60
        if self.is_expired() or (self.expiration_time - time.time() < grace_period_in_seconds):
            self.logger.debug(f"Refreshing auth token, which expires at {self.expiration_time}")
            self.expiration_time = time.time() + JwtSecretAuth.EXPIRATION_TIME_IN_SECONDS
            self.jwt_token = self._create_auth_token(self.jwt_secret, self.expiration_time).serialize()
            self.configure_session(session)

    def _create_auth_token(self, secret: str, expiration_time: float) -> JWT:
        """
        Create an admin auth token from the specified secret. By default, the token
        will be valid for 1 hour (3600 seconds).

        :param secret:
        :param valid_for:
        :return:
        """
        token = JWT(
            header={
                'alg': 'HS256'
            },
            claims={
                'sub': 'plastron',
                'iss': 'plastron',
                'exp': expiration_time,
                'role': 'fedoraAdmin'
            }
        )
        key = JWK(kty='oct', k=secret)
        token.make_signed_token(key)
        return token

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}('
                'jwt_secret=<REDACTED>, '
                f'self.expiration_time={self.expiration_time}, '
                'jwt_token=<REDACTED>'
                ')')


class ClientCertAuth(Auth):
    '''Auth for CLIENT_CERT, CLIENT_SECRET provided in config'''
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.client_cert = config['CLIENT_CERT']
        self.client_key = config['CLIENT_KEY']

    @classmethod
    def handles(cls, config: Dict[str, str]) -> bool:
        return ('CLIENT_CERT' in config) and ('CLIENT_KEY' in config)

    def configure_session(self, session: Session) -> None:
        session.cert = (self.client_cert, self.client_key)

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}('
                f'client_cert={self.client_cert!r},'
                f'client_key=<REDACTED>'
                f')')


class FedoraUserAuth(Auth):
    '''Auth for FEDORA_USER, FEDORA_PASSWORD provided in config'''
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.fedora_user = config['FEDORA_USER']
        self.fedora_password = config['FEDORA_PASSWORD']

    @classmethod
    def handles(cls, config: Dict[str, str]) -> bool:
        return 'FEDORA_USER' in config and 'FEDORA_PASSWORD' in config

    def configure_session(self, session: Session) -> None:
        session.auth = (self.fedora_user, self.fedora_password)

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}('
                f'fedora_user={self.fedora_user!r},'
                f'fedora_password=<REDACTED>'
                f')')
