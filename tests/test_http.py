import pytest
from plastron.auth import ProvidedJwtTokenAuth
from plastron.http import Repository
from plastron.exceptions import RESTAPIException


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


class MockRepository(Repository):
    def __init__(self, config, ua_string=None, on_behalf_of=None):
        super().__init__(config, ua_string, on_behalf_of)

    def set_mock_response(self, mock_response):
        self.mock_response = mock_response

    def head(self, uri, **kwargs):
        return self.mock_response

    def post(self, url, **kwargs):
        return self.mock_response


class MockResponse:
    def __init__(self, status_code, headers=None, links=None):
        self.status_code = status_code
        self.headers = headers
        self.links = links


def test_get_description_uri_failed_response(repo_base_config):
    repo = MockRepository(repo_base_config)
    repo.set_mock_response(MockResponse(404))
    with pytest.raises(RESTAPIException):
        repo.get_description_uri('http://example.com/foo')


def test_get_description_uri_no_describedby(repo_base_config):
    repo = MockRepository(repo_base_config)
    repo.set_mock_response(MockResponse(200, links={}))
    description_uri = repo.get_description_uri('uri')
    assert 'uri' == description_uri


def test_get_description_uri_no_describedby(repo_base_config):
    repo = MockRepository(repo_base_config)
    links = {'describedby': {'url': 'describedby_url'}}
    repo.set_mock_response(MockResponse(200, links=links))

    description_uri = repo.get_description_uri('uri')
    assert 'describedby_url' == description_uri


def test_create_with_no_describedby(repo_base_config):
    repo = MockRepository(repo_base_config)
    headers = {'Location': 'location_url'}
    links = {}
    repo.set_mock_response(MockResponse(201, headers=headers, links=links))

    resource_uri = repo.create()
    assert 'location_url' == resource_uri.uri
    assert 'location_url' == resource_uri.description_uri


def test_create_with_describedby(repo_base_config):
    repo = MockRepository(repo_base_config)
    headers = {'Location': 'location_url'}
    links = {'describedby': {'url': 'describedby_url'}}
    repo.set_mock_response(MockResponse(201, headers=headers, links=links))

    resource_uri = repo.create()
    assert 'location_url' == resource_uri.uri
    assert 'describedby_url' == resource_uri.description_uri


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


def test_repository_auth(repo_base_config):
    repository = Repository(repo_base_config)

    assert isinstance(repository.auth, ProvidedJwtTokenAuth)
    session = repository.session
    assert session.headers.get('Authorization')
    assert session.headers['Authorization'] == 'Bearer abcd-1234'


def test_repository_contains_uri(repo_base_config):
    repo_base_config['REPO_EXTERNAL_URL'] = 'http://external-host.com:8080/fcrepo/rest'
    repository = Repository(repo_base_config)

    assert repository.contains(None) is False
    assert repository.contains('') is False
    assert repository.contains('/not_in_repo') is False

    assert repository.contains(repository.endpoint) is True

    in_repo_uri = repository.endpoint + '/foo/bar'
    assert repository.contains(in_repo_uri) is True

    external_repo_uri = repo_base_config['REPO_EXTERNAL_URL']
    assert repository.contains(external_repo_uri) is True

    in_external_repo_uri = external_repo_uri + '/foo/bar'
    assert repository.contains(in_external_repo_uri) is True


def test_repository_contains_uri_without_repo_external_url(repo_base_config):
    repository = Repository(repo_base_config)

    assert repository.contains(None) is False
    assert repository.contains('') is False
    assert repository.contains('/not_in_repo') is False

    assert repository.contains(repository.endpoint) is True

    in_repo_uri = repository.endpoint + '/foo/bar'
    assert repository.contains(in_repo_uri) is True


def test_repository_repo_path(repo_base_config):
    # Without REPO_EXTERNAL_URL
    repository = Repository(repo_base_config)

    assert repository.repo_path(None) is None
    assert '' == repository.repo_path('')

    # URLs not in repository are returned unchanged (shouldn't happen)
    assert 'http://example.com/foo/bar' == repository.repo_path('http://example.com/foo/bar')

    # URLs starting with REST endpoint should be have endpoint prefix removed
    rest_endpoint = repo_base_config['REST_ENDPOINT']
    resource_uri = rest_endpoint + '/foo/bar'
    assert '/foo/bar' == repository.repo_path(resource_uri)

    # With REPO_EXTERNAL_URL
    repo_external_url = 'http://external-host.com:8080/fcrepo/rest'
    repo_base_config['REPO_EXTERNAL_URL'] = repo_external_url
    repository = Repository(repo_base_config)

    external_resource_uri = repo_external_url + '/baz/quuz'
    assert '/baz/quuz' == repository.repo_path(external_resource_uri)
