import datetime
import json
import pytest
import re
import time
from datetime import timedelta
from freezegun import freeze_time
from jwcrypto.jwk import JWK
from jwcrypto.jwt import JWT
from requests import Session
from plastron.exceptions import ConfigError

from plastron.auth import AuthFactory, ProvidedJwtTokenAuth, JwtSecretAuth, ClientCertAuth, FedoraUserAuth


def test_auth_factory_no_config():
    with pytest.raises(ValueError):
        config = None
        auth = AuthFactory.create(config)


def test_auth_factory_bad_config():
    with pytest.raises(ValueError):
        config = {'not_an_auth_key': 'not_an_auth_value'}
        auth = AuthFactory.create(config)


def test_auth_factory_provided_jwt_default_credentials():
    config = {'AUTH_TOKEN': 'abcd-1234'}
    auth = AuthFactory.create(config)
    assert isinstance(auth, ProvidedJwtTokenAuth)

    session = Session()
    session.batch_mode = False
    auth.configure_session(session)

    assert session.headers.get('Authorization')
    assert session.headers['Authorization'] == 'Bearer abcd-1234'


def test_auth_factory_provided_jwt_batch_credentials():
    config = {'AUTH_TOKEN': 'abcd-1234', 'BATCH_MODE': {'AUTH_TOKEN': 'batch-abcd-1234'}}
    auth = AuthFactory.create(config)
    session = Session()
    session.batch_mode = True
    auth.configure_session(session)

    assert session.headers.get('Authorization')
    assert session.headers['Authorization'] == 'Bearer batch-abcd-1234'


def test_auth_factory_provided_jwt_no_batch_credentials():
    config = {'AUTH_TOKEN': 'abcd-1234'}
    auth = AuthFactory.create(config)
    session = Session()
    session.batch_mode = True

    with pytest.raises(ConfigError):
        auth.configure_session(session)


def test_auth_factory_jwt_secret():
    config = {'JWT_SECRET': '833eba93802fdfce0e3d852b0bcb624f974551864e31e5d57920471f4a6a77e7'}
    auth = AuthFactory.create(config)
    assert isinstance(auth, JwtSecretAuth)

    session = Session()
    auth.configure_session(session)

    assert session.headers.get('Authorization')
    assert session.headers['Authorization'] == f'Bearer {auth.jwt_token}'


def test_auth_factory_client_cert():
    config = {'CLIENT_CERT': 'client-cert', 'CLIENT_KEY': 'abcd-1234'}
    auth = AuthFactory.create(config)
    assert isinstance(auth, ClientCertAuth)

    session = Session()
    auth.configure_session(session)

    assert session.cert == ('client-cert', 'abcd-1234')


def test_auth_factory_fedora_user():
    config = {'FEDORA_USER': 'user', 'FEDORA_PASSWORD': 'password'}
    auth = AuthFactory.create(config)
    assert isinstance(auth, FedoraUserAuth)

    session = Session()
    auth.configure_session(session)
    assert session.auth == (config['FEDORA_USER'], config['FEDORA_PASSWORD'])


def test_auth_precedence_order():
    # Verify that AuthFactory uses the following precedence order
    # 1) ProvidedJwtTokenAuth
    # 2) JwtSecretAuth
    # 3) ClientCertAuth
    # 4) FedoraUserAuth
    provided_jwt_config = {'AUTH_TOKEN': 'abcd-1234'}
    jwt_secret_config = {'JWT_SECRET': '833eba93802fdfce0e3d852b0bcb624f974551864e31e5d57920471f4a6a77e7'}
    client_cert_config = {'CLIENT_CERT': 'client-cert', 'CLIENT_KEY': 'abcd-1234'}
    fedora_user_config = {'FEDORA_USER': 'user', 'FEDORA_PASSWORD': 'password'}

    config = {}
    config.update(provided_jwt_config)
    config.update(jwt_secret_config)
    config.update(client_cert_config)
    config.update(fedora_user_config)
    auth = AuthFactory.create(config)
    assert isinstance(auth, ProvidedJwtTokenAuth)

    del config['AUTH_TOKEN']
    auth = AuthFactory.create(config)
    assert isinstance(auth, JwtSecretAuth)

    del config['JWT_SECRET']
    auth = AuthFactory.create(config)
    assert isinstance(auth, ClientCertAuth)

    del config['CLIENT_CERT']
    del config['CLIENT_KEY']
    auth = AuthFactory.create(config)
    assert isinstance(auth, FedoraUserAuth)


def test_auth_jwt_secret_expiration():
    initial_datetime = datetime.datetime(year=2021, month=2, day=19,
                                         hour=13, minute=0, second=0)
    with freeze_time(initial_datetime) as frozen_datetime:
        auth = JwtSecretAuth({'JWT_SECRET': '833eba93802fdfce0e3d852b0bcb624f974551864e31e5d57920471f4a6a77e7'})
        assert not auth.is_expired()

        # JwtSecretAuth tokens valid up to one houw
        frozen_datetime.tick(delta=datetime.timedelta(hours=1))
        assert not auth.is_expired()

        # JwtSecretAuth tokens expire after one hour
        frozen_datetime.tick(delta=datetime.timedelta(seconds=1))
        assert auth.is_expired()


def test_auth_jwt_secret_tokens_can_be_refreshed():
    initial_datetime = datetime.datetime(year=2021, month=2, day=19,
                                         hour=13, minute=0, second=0)
    with freeze_time(initial_datetime) as frozen_datetime:
        session = Session()
        jwt_secret = '833eba93802fdfce0e3d852b0bcb624f974551864e31e5d57920471f4a6a77e7'
        auth = JwtSecretAuth({'JWT_SECRET': jwt_secret})
        auth.configure_session(session)

        # JwtSecretAuth tokens expire after one hour
        frozen_datetime.tick(delta=datetime.timedelta(hours=1, seconds=1))
        assert auth.is_expired()

        # Refresh token
        auth.refresh_auth(session)
        assert not auth.is_expired()

        # Verify that session is using refreshed token
        session_jwt_token = re.search('Bearer (.*)', session.headers['Authorization']).group(1)
        expiration_datetime = expiration_datetime_from_jwt_token(session_jwt_token, jwt_secret)

        expected_expiration_time = datetime.datetime.fromtimestamp(time.time())  + timedelta(hours=1)

        assert expected_expiration_time == expiration_datetime


def expiration_datetime_from_jwt_token(jwt_token: str, jwt_secret: str) -> datetime:
    key = JWK(kty='oct', k=jwt_secret)
    jwt = JWT(jwt=jwt_token, key=key)

    jwt_claims_json = jwt.claims
    claims = json.loads(jwt_claims_json)
    expiration_datetime = datetime.datetime.fromtimestamp(claims['exp'])
    return expiration_datetime
