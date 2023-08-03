from base64 import b64encode

import pytest
from requests import Session, Request
from requests.auth import HTTPBasicAuth
from requests_jwtauth import HTTPBearerAuth, JWTSecretAuth

from plastron.client.auth import ClientCertAuth, get_authenticator


@pytest.fixture
def get_request():
    return Request(method='get', url='http://localhost:9999/')


def test_auth_factory_no_config():
    with pytest.raises(TypeError):
        get_authenticator(None)  # noqa


def test_auth_factory_empty_config():
    assert get_authenticator({}) is None


def test_auth_factory_provided_jwt_default_credentials(get_request):
    config = {'AUTH_TOKEN': 'abcd-1234'}
    auth = get_authenticator(config)
    assert isinstance(auth, HTTPBearerAuth)

    session = Session()
    session.batch_mode = False
    session.auth = auth
    r = session.prepare_request(get_request)

    assert 'Authorization' in r.headers
    assert r.headers['Authorization'] == 'Bearer abcd-1234'


def test_auth_factory_jwt_secret(get_request):
    # noinspection SpellCheckingInspection
    config = {'JWT_SECRET': '833eba93802fdfce0e3d852b0bcb624f974551864e31e5d57920471f4a6a77e7'}
    auth = get_authenticator(config)
    assert isinstance(auth, JWTSecretAuth)

    session = Session()
    session.auth = auth
    r = session.prepare_request(get_request)

    assert 'Authorization' in r.headers
    assert r.headers['Authorization'] == f'Bearer {auth.token.serialize()}'


def test_auth_factory_client_cert(get_request):
    config = {'CLIENT_CERT': 'client-cert', 'CLIENT_KEY': 'abcd-1234'}
    auth = get_authenticator(config)
    assert isinstance(auth, ClientCertAuth)

    session = Session()
    session.auth = auth
    r = session.prepare_request(get_request)

    assert r.cert == ('client-cert', 'abcd-1234')


def test_auth_factory_fedora_user(get_request):
    config = {'FEDORA_USER': 'user', 'FEDORA_PASSWORD': 'password'}
    auth = get_authenticator(config)
    assert isinstance(auth, HTTPBasicAuth)

    session = Session()
    session.auth = auth
    r = session.prepare_request(get_request)
    basic_credentials = b64encode(f"{config['FEDORA_USER']}:{config['FEDORA_PASSWORD']}".encode()).decode()
    assert r.headers['Authorization'] == f'Basic {basic_credentials}'


def test_auth_precedence_order():
    # Verify that AuthFactory uses the following precedence order
    # 1) ProvidedJwtTokenAuth
    # 2) JwtSecretAuth
    # 3) ClientCertAuth
    # 4) FedoraUserAuth
    provided_jwt_config = {'AUTH_TOKEN': 'abcd-1234'}
    # noinspection SpellCheckingInspection
    jwt_secret_config = {'JWT_SECRET': '833eba93802fdfce0e3d852b0bcb624f974551864e31e5d57920471f4a6a77e7'}
    client_cert_config = {'CLIENT_CERT': 'client-cert', 'CLIENT_KEY': 'abcd-1234'}
    fedora_user_config = {'FEDORA_USER': 'user', 'FEDORA_PASSWORD': 'password'}

    config = {}
    config.update(provided_jwt_config)
    config.update(jwt_secret_config)
    config.update(client_cert_config)
    config.update(fedora_user_config)
    auth = get_authenticator(config)
    assert isinstance(auth, HTTPBearerAuth)

    del config['AUTH_TOKEN']
    auth = get_authenticator(config)
    assert isinstance(auth, JWTSecretAuth)

    del config['JWT_SECRET']
    auth = get_authenticator(config)
    assert isinstance(auth, ClientCertAuth)

    del config['CLIENT_CERT']
    del config['CLIENT_KEY']
    auth = get_authenticator(config)
    assert isinstance(auth, HTTPBasicAuth)
