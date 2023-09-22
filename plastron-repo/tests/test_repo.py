import pytest

from plastron.repo import Repository, RepositoryResource, ContainerResource


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
