import pytest

from plastron.repo import Repository, RepositoryResource, ContainerResource, Tombstone


@pytest.fixture
def repository():
    return Repository.from_url('http://localhost:8080/fcrepo/rest')


def test_repo_from_url():
    repo = Repository.from_url('http://localhost:8080/fcrepo/rest')
    assert repo.endpoint.url == 'http://localhost:8080/fcrepo/rest'


@pytest.mark.parametrize(
    ('input_path', 'input_class', 'expected_path', 'expected_class'),
    [
        ('http://localhost:8080/fcrepo/rest/foo', None, '/foo', RepositoryResource),
        ('/foo', None, '/foo', RepositoryResource),
        ('/foo', ContainerResource, '/foo', ContainerResource),
    ]
)
def test_get_resource(repository, input_path, input_class, expected_path, expected_class):
    resource = repository.get_resource(path=input_path, resource_class=input_class)
    assert resource.path == expected_path
    assert isinstance(resource, expected_class)


@pytest.mark.parametrize(
    ('input_value', 'expected_path', 'expected_class'),
    [
        ('http://localhost:8080/fcrepo/rest/foo', '/foo', RepositoryResource),
        ('/foo', '/foo', RepositoryResource),
        (slice('/foo', ContainerResource), '/foo', ContainerResource),
    ]
)
def test_get_item(repository, input_value, expected_path, expected_class):
    resource = repository[input_value]
    assert resource.path == expected_path
    assert isinstance(resource, expected_class)


class MockGoneResponse:
    ok = False
    status_code = 410
    reason = 'Gone'
    headers = {}
    links = {}


def test_walk_exclude_tombstones(repository, monkeypatch_request):
    origin = RepositoryResource(repository, '/foo')
    monkeypatch_request(MockGoneResponse)
    assert len(list(origin.walk(include_tombstones=False))) == 0


def test_walk_include_tombstones(repository, monkeypatch_request):
    origin = RepositoryResource(repository, '/foo')
    monkeypatch_request(MockGoneResponse)
    resource = next(origin.walk(include_tombstones=True))
    assert isinstance(resource, Tombstone)
