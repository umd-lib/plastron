from http import HTTPStatus
from unittest.mock import Mock

import pytest
from plastron.repo import ACLResource, Repository, RepositoryError, RepositoryResource


class MockResponse:
    def __init__(self, status_code, links=None):
        self.status_code = status_code
        self.ok = status_code < HTTPStatus.BAD_REQUEST
        self.reason = HTTPStatus(status_code).phrase
        self.headers = {}
        self.links = links or {}


@pytest.fixture
def resource():
    repo = Repository.from_url('http://localhost:8080/fcrepo/rest')
    return RepositoryResource(repo, '/resource')


def test_get_acl_returns_linked_acl_resource(resource):
    resource.client.head = Mock(
        side_effect=[
            MockResponse(HTTPStatus.OK, {'acl': {'rel': 'acl', 'url': '/fcrepo/rest/resource/fcr:acl'}}),
            MockResponse(HTTPStatus.OK),
        ]
    )

    acl_resource = resource.get_acl()

    assert isinstance(acl_resource, ACLResource)
    assert acl_resource.url == 'http://localhost:8080/fcrepo/rest/resource/fcr:acl'
    assert resource.client.head.call_count == 2


def test_get_acl_returns_none_without_acl_link(resource):
    resource.client.head = Mock(return_value=MockResponse(HTTPStatus.OK))

    assert resource.get_acl() is None
    resource.client.head.assert_called_once_with(resource.url)


def test_get_acl_returns_none_when_linked_acl_is_not_found(resource):
    resource.client.head = Mock(
        side_effect=[
            MockResponse(HTTPStatus.OK, {'acl': {'rel': 'acl', 'url': '/fcrepo/rest/resource/fcr:acl'}}),
            MockResponse(HTTPStatus.NOT_FOUND),
        ]
    )

    assert resource.get_acl() is None


@pytest.mark.parametrize('status_code', [HTTPStatus.FORBIDDEN, HTTPStatus.INTERNAL_SERVER_ERROR])
def test_get_acl_raises_for_other_linked_acl_errors(resource, status_code):
    resource.client.head = Mock(
        side_effect=[
            MockResponse(HTTPStatus.OK, {'acl': {'rel': 'acl', 'url': '/fcrepo/rest/resource/fcr:acl'}}),
            MockResponse(status_code),
        ]
    )

    with pytest.raises(RepositoryError):
        resource.get_acl()


def test_get_acl_wraps_head_exceptions(resource):
    resource.client.head = Mock(side_effect=RuntimeError('connection failed'))

    with pytest.raises(RepositoryError) as error:
        resource.get_acl()

    assert isinstance(error.value.__cause__, RuntimeError)
