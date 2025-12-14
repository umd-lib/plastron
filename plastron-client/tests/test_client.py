import logging
import re
from http.client import RemoteDisconnected
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from requests import Session, Request
from requests.exceptions import ConnectionError
from requests_jwtauth import HTTPBearerAuth

from plastron.client import Endpoint, Client, ClientError
from plastron.client.auth import ClientCertAuth
from plastron.client.utils import random_slug, ResourceURI


@pytest.fixture()
def client(endpoint):
    return Client(endpoint=endpoint)


@pytest.fixture()
def repo_base_config():
    """Required parameters for Repository configuration"""
    return {
        'REST_ENDPOINT': 'http://base-host.com:8080/rest',
        'RELPATH': '/pcdm',
        'LOG_DIR': '/logs',
        'AUTH_TOKEN': 'abcd-1234'
    }


class MockOKResponse:
    ok = True
    status_code = 200
    reason = 'OK'


class MockNotFoundResponse:
    ok = False
    status_code = 404
    reason = 'Not Found'


class MockNoDescribedbyHeaderResponse(MockOKResponse):
    links = {}


class MockDescribedbyHeaderResponse(MockOKResponse):
    links = {'describedby': {'url': 'describedby_url'}}


def test_get_description_uri_failed_response(monkeypatch_request, client):
    monkeypatch_request(MockNotFoundResponse)
    with pytest.raises(ClientError):
        client.get_description_uri('http://example.com/repo/foo')


def test_get_description_uri_no_describedby(monkeypatch_request, client):
    monkeypatch_request(MockNoDescribedbyHeaderResponse)
    description_uri = client.get_description_uri('uri')
    assert 'uri' == description_uri


def test_get_description_uri(monkeypatch_request, client):
    monkeypatch_request(MockDescribedbyHeaderResponse)
    description_uri = client.get_description_uri('uri')
    assert 'describedby_url' == description_uri


class MockCreatedResponse(MockOKResponse):
    status_code = 201
    reason = 'Created'
    headers = {'Location': 'location_url'}
    links = {}


class MockCreatedResponseDescribedby(MockCreatedResponse):
    links = {'describedby': {'url': 'describedby_url'}}


def test_create_with_no_describedby(monkeypatch_request, client):
    monkeypatch_request(MockCreatedResponse)
    resource_uri = client.create()
    assert 'location_url' == resource_uri.uri
    assert 'location_url' == resource_uri.description_uri


def test_create_with_describedby(monkeypatch_request, client):
    monkeypatch_request(MockCreatedResponseDescribedby)
    resource_uri = client.create()
    assert 'location_url' == resource_uri.uri
    assert 'describedby_url' == resource_uri.description_uri


def test_repository_auth(endpoint):
    client = Client(endpoint=endpoint, auth=HTTPBearerAuth('abcd-1234'))

    assert isinstance(client.session.auth, HTTPBearerAuth)
    assert client.session.auth.token == 'abcd-1234'


def test_repository_contains_uri():
    repo = Endpoint(url='http://localhost:8080/repo')

    assert repo.contains('') is False
    assert repo.contains('/not_in_repo') is False
    assert repo.contains(repo.url) is True
    assert repo.contains(repo.url + '/foo/bar') is True


def test_repository_repo_path():
    endpoint = Endpoint(url='http://localhost:8080/repo')

    assert endpoint.repo_path(None) is None
    assert '' == endpoint.repo_path('')

    # URLs not in repository are returned unchanged (shouldn't happen)
    assert 'http://example.com/foo/bar' == endpoint.repo_path('http://example.com/foo/bar')

    # URLs starting with REST endpoint should have endpoint prefix removed
    resource_uri = endpoint.url + '/foo/bar'
    assert '/foo/bar' == endpoint.repo_path(resource_uri)


def test_random_slug_default():
    slug = random_slug()
    assert len(slug) == 8
    assert re.match('^[a-zA-Z0-9_=-]+$', slug)


def test_random_slug_with_length():
    slug = random_slug(9)
    # the length of the base64-encoded slug is always 4/3 * number of bytes
    assert len(slug) == 12
    assert re.match('^[a-zA-Z0-9_=-]+$', slug)


def test_resource_uri():
    r = ResourceURI(uri='foo', description_uri='bar')
    assert r.uri == 'foo'
    assert r.description_uri == 'bar'
    assert str(r) == 'foo'


def test_client_cert_auth():
    auth = ClientCertAuth(key='abcd-1234', cert='client-cert')
    get_request = Request(method='get', url='http://localhost:9999/')
    session = Session()
    session.auth = auth
    r = session.prepare_request(get_request)

    assert r.cert == ('client-cert', 'abcd-1234')


def test_client_ua_string(endpoint):
    client = Client(endpoint=endpoint, ua_string='test/1.2.3')
    assert client.session.headers['User-Agent'] == 'test/1.2.3'


def test_client_delegated_user(endpoint):
    client = Client(endpoint=endpoint, on_behalf_of='josef_k')
    assert client.session.headers['On-Behalf-Of'] == 'josef_k'


def test_get_graph_not_found(monkeypatch_request, client):
    monkeypatch_request(MockNotFoundResponse)
    with pytest.raises(ClientError):
        client.get_graph('http://localhost:9999/fcrepo/rest/123')


def test_client_connection_error(client, monkeypatch):
    error = RemoteDisconnected('Remote end closed connection without response')
    mock_session = MagicMock(spec=Session)
    mock_session.request.side_effect = ConnectionError('Connection aborted.', error)
    monkeypatch.setattr(client, 'session', mock_session)
    with pytest.raises(RuntimeError) as e:
        client.get(client.endpoint.url)
    assert str(e.value) == 'Connection error: Connection aborted. Remote end closed connection without response'


def test_paths_to_create():
    mock_session = MagicMock(spec=Session)
    mock_session.request.return_value = MockOKResponse()

    client = Client(
        endpoint=Endpoint('http://example.com/fcrepo/rest'),
        session=mock_session,
    )
    assert client.paths_to_create(Path('/foo')) == []


def test_client_is_reachable(endpoint):
    session = MagicMock(spec=Session)
    session.request.return_value = MockOKResponse()
    client = Client(endpoint=endpoint, session=session)
    assert client.is_reachable()


def test_client_is_not_reachable_not_found(endpoint):
    session = MagicMock(spec=Session)
    session.request.return_value = MockNotFoundResponse()
    client = Client(endpoint=endpoint, session=session)
    assert not client.is_reachable()


def test_client_is_not_reachable_connection_error(endpoint):
    session = MagicMock(spec=Session)
    session.request.side_effect = ConnectionError('Connection aborted.')
    client = Client(endpoint=endpoint, session=session)
    assert not client.is_reachable()


def test_client_test_connection_success(endpoint, caplog):
    caplog.set_level(logging.INFO)
    session = MagicMock(spec=Session)
    session.request.return_value = MockOKResponse()
    client = Client(endpoint=endpoint, session=session)
    client.test_connection()
    assert 'Connection successful.' in caplog.text


def test_client_test_connection_error(endpoint):
    session = MagicMock(spec=Session)
    session.request.side_effect = ConnectionError('Connection aborted.')
    client = Client(endpoint=endpoint, session=session)
    with pytest.raises(ConnectionError):
        client.test_connection()
