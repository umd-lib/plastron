from typing import Type
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from plastron.client import Client, Endpoint
from plastron.files import StringSource
from plastron.jobs import FileSpec, FileGroup
from plastron.repo import Repository, ResourceType
from plastron.repo.pcdm import PCDMObjectResource


class MockRepo(Repository):
    def create(self, resource_class: Type[ResourceType] = None, **kwargs) -> ResourceType:
        return resource_class(repo=self, path=str(uuid4()))


@pytest.fixture
def mock_client():
    return MagicMock(spec=Client, endpoint=Endpoint('http://localhost:8080/rest'))


@pytest.fixture
def mock_repo(mock_client):
    return MockRepo(client=mock_client)


@pytest.fixture
def string_source() -> StringSource:
    return StringSource('foobar', filename='foo.txt', mimetype='text/plain')


@pytest.fixture
def empty_file_group():
    return FileGroup(rootname='foo')


@pytest.fixture
def single_file_group(string_source):
    return FileGroup(rootname='foo', files=[FileSpec(name='foo.txt', source=string_source)])


@pytest.fixture
def multiple_files_group(string_source):
    return FileGroup(
        rootname='foo', files=[
            FileSpec(name='foo.txt', source=string_source),
            FileSpec(name='foo.asc', source=string_source),
        ]
    )


def test_create_page(mock_repo, empty_file_group):
    resource = mock_repo.create(PCDMObjectResource)
    assert len(resource.member_urls) == 0
    resource.create_page(number=1, file_group=empty_file_group)
    assert len(resource.member_urls) == 1


def test_create_file(mock_repo, string_source):
    resource = mock_repo.create(PCDMObjectResource)
    assert len(resource.file_urls) == 0
    resource.create_file(string_source)
    assert len(resource.file_urls) == 1


def test_create_page_with_file(mock_repo, single_file_group):
    resource = mock_repo.create(PCDMObjectResource)
    page_resource = resource.create_page(number=1, file_group=single_file_group)
    # expecting 1 member...
    assert len(resource.member_urls) == 1
    # ...with 0 directly attached files
    assert len(resource.file_urls) == 0
    # expecting 1 file attached to the page
    assert len(page_resource.file_urls) == 1


def test_create_page_with_multiple_files(mock_repo, multiple_files_group):
    resource = mock_repo.create(PCDMObjectResource)
    page_resource = resource.create_page(number=1, file_group=multiple_files_group)
    # expecting 1 member...
    assert len(resource.member_urls) == 1
    # ...with 0 directly attached files
    assert len(resource.file_urls) == 0
    # expecting 2 files attached to the page
    assert len(page_resource.file_urls) == 2


def test_create_multiple_pages_with_files(mock_repo, single_file_group, multiple_files_group, empty_file_group):
    resource = mock_repo.create(PCDMObjectResource)
    page1_resource = resource.create_page(number=1, file_group=multiple_files_group)
    page2_resource = resource.create_page(number=2, file_group=single_file_group)
    page3_resource = resource.create_page(number=3, file_group=empty_file_group)
    # expecting 3 members...
    assert len(resource.member_urls) == 3
    # ...with 0 directly attached files
    assert len(resource.file_urls) == 0
    # expecting 2 files attached to page 1
    assert len(page1_resource.file_urls) == 2
    # expecting 1 file attached to page 2
    assert len(page2_resource.file_urls) == 1
    # expecting 0 files attached to page 3
    assert len(page3_resource.file_urls) == 0
