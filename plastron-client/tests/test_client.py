import re
from http.client import RemoteDisconnected
from unittest.mock import MagicMock

import pytest
from requests import Session, Request
from requests.exceptions import ConnectionError
from requests_jwtauth import HTTPBearerAuth

from plastron.client import Endpoint, Client, random_slug, ResourceURI, ClientError
from plastron.client.auth import ClientCertAuth


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


@pytest.fixture()
def repo_forwarded_config(repo_base_config):
    repo_base_config['REPO_EXTERNAL_URL'] = 'https://forwarded-host.com/'
    return repo_base_config


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


"""
def test_process_description_uri_no_describedby_no_forwards(repo_base_config):
    repo = MockRepository(repo_base_config)
    response = MockResponse(201, links={})

    description_uri = repo.process_description_uri('uri', response)
    assert 'uri' == description_uri


def test_process_description_uri_with_describedby_no_forwards(repo_base_config):
    repo = MockRepository(repo_base_config)
    response = MockResponse(201, links={'describedby': {'url': 'describedby_url'}})

    description_uri = repo.process_description_uri('uri', response)
    assert 'describedby_url' == description_uri


def test_process_description_uri_no_describedby_with_forwards(repo_forwarded_config):
    repo = MockRepository(repo_forwarded_config)
    response = MockResponse(201, links={})

    description_uri = repo.process_description_uri('http://base-host.com:8080/rest', response)
    assert 'https://forwarded-host.com/rest' == description_uri


def test_process_description_uri_with_describedby_with_forwards(repo_forwarded_config):
    repo = MockRepository(repo_forwarded_config)
    response = MockResponse(201, links={'describedby': {'url': 'http://base-host.com:8080/rest/abc'}})

    description_uri = repo.process_description_uri('http://base-host.com:8080/rest', response)
    assert 'https://forwarded-host.com/rest/abc' == description_uri


def test_create_with_describedby_and_forwarded_params(repo_forwarded_config):
    repo = MockRepository(repo_forwarded_config)
    headers = {'Location': 'location_url'}
    links = {'describedby': {'url': 'http://localhost:8080/'}}
    repo.set_mock_response(MockResponse(201, headers=headers, links=links))

    resource_uri = repo.create()

    assert resource_uri.uri.startswith('https://forwarded-host.com/')
    assert resource_uri.description_uri.startswith('https://forwarded-host.com/')


def test_forwarded_params_sets_headers(repo_forwarded_config):
    config = repo_forwarded_config

    repository = Repository(config)

    assert repository.is_forwarded()
    assert 'X-Forwarded-Host' in repository.session.headers.keys()
    assert 'X-Forwarded-Proto' in repository.session.headers.keys()
    assert 'forwarded-host.com' == repository.session.headers['X-Forwarded-Host']
    assert 'https' == repository.session.headers['X-Forwarded-Proto']


def test_forwarded_params_are_optional(repo_base_config):
    config = repo_base_config

    repository = Repository(config)

    assert not repository.is_forwarded()
    assert not ('X-Forwarded-Host' in repository.session.headers.keys())
    assert not ('X-Forwarded-Proto' in repository.session.headers.keys())


def test_forwarded_endpoint_should_include_port_if_present(repo_base_config):
    config = repo_base_config
    config['REPO_EXTERNAL_URL'] = 'https://forwarded-host.com:1234/'

    repository = Repository(config)

    assert repository.is_forwarded()
    assert ('X-Forwarded-Host' in repository.session.headers.keys())
    assert ('X-Forwarded-Proto' in repository.session.headers.keys())
    assert 'forwarded-host.com:1234' == repository.session.headers['X-Forwarded-Host']
    assert 'https' == repository.session.headers['X-Forwarded-Proto']

    assert repository.forwarded_endpoint == 'https://forwarded-host.com:1234/rest'


def test_forwarded_params_not_used_if_rest_endpoint_and_fcrepo_base_url_match(repo_base_config):
    config = repo_base_config
    repo_base_config['REPO_EXTERNAL_URL'] = 'http://base-host.com:8080/'

    repository = Repository(config)

    assert not repository.is_forwarded()
    assert not ('X-Forwarded-Host' in repository.session.headers.keys())
    assert not ('X-Forwarded-Proto' in repository.session.headers.keys())


def test_undo_forward_does_nothing_when_not_forwarding(repo_base_config):
    repository = Repository(repo_base_config)

    assert not repository.is_forwarded()

    url = 'http://base-host.com:8080/rest/ab/cd/def#1234'
    assert 'http://base-host.com:8080/rest/ab/cd/def#1234' == repository.undo_forward(url)


def test_undo_forward_when_forwarding(repo_forwarded_config):
    repository = Repository(repo_forwarded_config)

    assert repository.is_forwarded()

    forwarded_url = 'https://forwarded-host.com/rest/ab/cd/def'
    assert 'http://base-host.com:8080/rest/ab/cd/def' == repository.undo_forward(forwarded_url)

    # Should include fragment when undoing
    forwarded_url = 'https://forwarded-host.com/rest/ab/cd/def#1234'
    assert 'http://base-host.com:8080/rest/ab/cd/def#1234' == repository.undo_forward(forwarded_url)

    assert 'http://base-host.com:8080/rest/ab/cd/def#1234' == repository.undo_forward(forwarded_url)
"""


def test_repository_auth(endpoint):
    client = Client(endpoint=endpoint, auth=HTTPBearerAuth('abcd-1234'))

    assert isinstance(client.session.auth, HTTPBearerAuth)
    assert client.session.auth.token == 'abcd-1234'


def test_repository_contains_uri():
    repo = Endpoint(url='http://localhost:8080/repo', external_url='http://example.com/repo')

    assert repo.contains('') is False
    assert repo.contains('/not_in_repo') is False
    assert repo.contains(repo.url) is True
    assert repo.contains(repo.url + '/foo/bar') is True
    assert repo.contains(repo.external_url) is True
    assert repo.contains(repo.external_url + '/foo/bar') is True


def test_repository_contains_uri_without_repo_external_url():
    repo = Endpoint(url='http://localhost:8080/repo')

    assert repo.contains('') is False
    assert repo.contains('/not_in_repo') is False
    assert repo.contains(repo.url) is True
    assert repo.contains(repo.url + '/foo/bar') is True


def test_repository_repo_path():
    # Without REPO_EXTERNAL_URL
    repo = Endpoint(url='http://localhost:8080/repo')

    assert repo.repo_path(None) is None
    assert '' == repo.repo_path('')

    # URLs not in repository are returned unchanged (shouldn't happen)
    assert 'http://example.com/foo/bar' == repo.repo_path('http://example.com/foo/bar')

    # URLs starting with REST endpoint should have endpoint prefix removed
    resource_uri = repo.url + '/foo/bar'
    assert '/foo/bar' == repo.repo_path(resource_uri)

    # With REPO_EXTERNAL_URL
    repo.external_url = 'http://external-host.com:8080/fcrepo/rest'
    assert '/baz/quuz' == repo.repo_path(repo.external_url + '/baz/quuz')


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


def test_repo_external_url():
    repo = Endpoint(url='http://localhost:8080/repo', external_url='https://example.com/repo')
    client = Client(endpoint=repo)
    assert client.session.headers['X-Forwarded-Host'] == 'example.com'
    assert client.session.headers['X-Forwarded-Proto'] == 'https'


def test_get_graph_not_found(monkeypatch_request, client):
    monkeypatch_request(MockNotFoundResponse)
    with pytest.raises(ClientError):
        client.get_graph('http://localhost:9999/fcrepo/rest/123')


@pytest.mark.parametrize(
    ('url', 'external_url', 'forwarded_host', 'forwarded_protocol'),
    [
        ('http://localhost:8080', 'http://fcrepo-local', 'fcrepo-local', 'http'),
        ('http://localhost:8080', 'https://fcrepo-local', 'fcrepo-local', 'https'),
        ('http://localhost:8080', 'http://fcrepo-local:8080', 'fcrepo-local:8080', 'http'),
    ]
)
def test_forwarded_headers(url, external_url, forwarded_host, forwarded_protocol):
    endpoint = Endpoint(url=url, external_url=external_url)
    client = Client(endpoint=endpoint)
    assert client.forwarded_host == forwarded_host
    assert client.forwarded_protocol == forwarded_protocol


def test_client_connection_error(client, monkeypatch):
    error = RemoteDisconnected('Remote end closed connection without response')
    mock_session = MagicMock(spec=Session)
    mock_session.request.side_effect = ConnectionError('Connection aborted.', error)
    monkeypatch.setattr(client, 'session', mock_session)
    with pytest.raises(RuntimeError) as e:
        client.get(client.endpoint.url)
    assert str(e.value) == 'Connection error: Connection aborted. Remote end closed connection without response'
