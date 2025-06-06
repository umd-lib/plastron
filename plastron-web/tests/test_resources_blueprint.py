from http import HTTPStatus
from unittest.mock import MagicMock

import pytest

from plastron.context import PlastronContext
from plastron.models import ContentModeledResource
from plastron.rdfmapping.validation import ValidationResultsDict, ValidationFailure
from plastron.repo import RepositoryResource, Repository, RepositoryError


@pytest.fixture
def mock_valid_object():
    obj = MagicMock(spec=ContentModeledResource)
    obj.validate.return_value = ValidationResultsDict()
    return obj


@pytest.fixture
def mock_invalid_object():
    obj = MagicMock(spec=ContentModeledResource)
    obj.validate.return_value = ValidationResultsDict({'foo': ValidationFailure()})
    return obj


@pytest.fixture
def mock_resource():
    resource = MagicMock(spec=RepositoryResource)
    resource.read.return_value = resource
    return resource


@pytest.fixture
def mock_repo(mock_resource):
    repo = MagicMock(spec=Repository)
    repo.get_resource.return_value = mock_resource
    return repo


@pytest.fixture
def mock_context(mock_repo):
    return MagicMock(spec=PlastronContext, repo=mock_repo)


@pytest.fixture
def app_client_with_context(app_client, mock_context):
    app_client.application.config['CONTEXT'] = mock_context
    return app_client


def test_update_resource_not_found(mock_repo, app_client_with_context):
    mock_repo.get_resource.side_effect = RepositoryError

    response = app_client_with_context.patch(
        '/resources/foo',
        headers={'Content-Type': 'application/sparql-update'},
        data='DELETE {} INSERT {} WHERE {}',
        query_string={'model': 'Item'},
    )
    assert response.status_code == HTTPStatus.NOT_FOUND


def test_update_missing_content_type(app_client_with_context):
    response = app_client_with_context.patch(
        '/resources/foo',
    )
    assert response.status_code == HTTPStatus.UNSUPPORTED_MEDIA_TYPE


def test_update_wrong_content_type(app_client_with_context):
    response = app_client_with_context.patch(
        '/resources/foo',
        headers={'Content-Type': 'text/plain'}
    )
    assert response.status_code == HTTPStatus.UNSUPPORTED_MEDIA_TYPE


def test_update_unknown_content_model(app_client_with_context):
    response = app_client_with_context.patch(
        '/resources/foo',
        headers={'Content-Type': 'application/sparql-update'},
        data='DELETE {} INSERT {} WHERE {}',
        query_string={'model': 'FAKE_MODEL_THAT_DOES_NOT_EXIST'},
    )
    assert response.status_code == HTTPStatus.BAD_REQUEST
    problem_detail = response.json
    assert problem_detail['status'] == HTTPStatus.BAD_REQUEST
    assert problem_detail['title'] == 'Unrecognized content-model'
    assert 'is not a recognized content-model name' in problem_detail['details']


def test_update_bad_sparql(mock_resource, mock_valid_object, app_client_with_context):
    mock_resource.describe.return_value = mock_valid_object

    response = app_client_with_context.patch(
        '/resources/foo',
        headers={'Content-Type': 'application/sparql-update'},
        data='this is not SPARQL Update!!!',
        query_string={'model': 'Item'},
    )
    assert response.status_code == HTTPStatus.BAD_REQUEST
    problem_detail = response.json
    assert problem_detail['status'] == HTTPStatus.BAD_REQUEST
    assert problem_detail['title'] == 'SPARQL Update problem'
    assert 'SPARQL Update Query parsing error' in problem_detail['details']


def test_valid_update(mock_resource, mock_valid_object, app_client_with_context):
    mock_resource.describe.return_value = mock_valid_object

    response = app_client_with_context.patch(
        '/resources/foo',
        headers={'Content-Type': 'application/sparql-update'},
        data='DELETE {} INSERT {} WHERE {}',
        query_string={'model': 'Item'},
    )
    assert response.status_code == HTTPStatus.NO_CONTENT


def test_update_repository_error(mock_resource, mock_valid_object, app_client_with_context):
    mock_resource.describe.return_value = mock_valid_object
    mock_resource.update.side_effect = RepositoryError

    response = app_client_with_context.patch(
        '/resources/foo',
        headers={'Content-Type': 'application/sparql-update'},
        data='DELETE {} INSERT {} WHERE {}',
        query_string={'model': 'Item'},
    )
    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR


def test_invalid_update(mock_resource, mock_invalid_object, app_client_with_context):
    mock_resource.describe.return_value = mock_invalid_object
    mock_resource.url = 'http://example.com/fcrepo/rest/foo'

    response = app_client_with_context.patch(
        '/resources/foo',
        headers={'Content-Type': 'application/sparql-update'},
        data='DELETE {} INSERT {} WHERE {}',
        query_string={'model': 'Item'},
    )
    assert response.status_code == HTTPStatus.BAD_REQUEST
    problem_detail = response.json
    assert problem_detail['status'] == HTTPStatus.BAD_REQUEST
    assert problem_detail['title'] == 'Content-model validation failed'
    assert 'validation error(s)' in problem_detail['details']
