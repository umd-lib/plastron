import pytest
from requests import Session

from plastron.auth.auth import AuthFactory, ProvidedJwtTokenAuth, JwtSecretAuth, ClientCertAuth, FedoraUserAuth


def test_auth_factory_no_config():
    with pytest.raises(ValueError):
        config = None
        auth = AuthFactory.create(config)


def test_auth_factory_bad_config():
    with pytest.raises(ValueError):
        config = {'not_an_auth_key': 'not_an_auth_value'}
        auth = AuthFactory.create(config)


def test_auth_factory_provided_jwt():
    config = {'AUTH_TOKEN': 'abcd-1234'}
    auth = AuthFactory.create(config)
    assert isinstance(auth, ProvidedJwtTokenAuth)

    session = Session()
    auth.configure_session(session)

    assert session.headers.get('Authorization')
    assert session.headers['Authorization'] == 'Bearer abcd-1234'


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

    session = Session
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
