from unittest.mock import MagicMock

import pytest

from plastron.client import Endpoint
from plastron.repo import RepositoryResource, Repository, RepositoryError
from plastron.repo.pcdm import PCDMObjectResource


@pytest.fixture
def mock_repo():
    return MagicMock(spec=Repository, endpoint=Endpoint('http://example.com/fcrepo'))


def test_convert_to(mock_repo):
    resource = RepositoryResource(mock_repo, '/foo')
    assert isinstance(resource, RepositoryResource)
    assert not isinstance(resource, PCDMObjectResource)
    pcdm_resource = resource.convert_to(PCDMObjectResource)
    assert isinstance(pcdm_resource, PCDMObjectResource)
    assert resource.repo == pcdm_resource.repo
    assert resource.path == pcdm_resource.path


class NotARepoResource:
    pass


def test_convert_to_unsuitable_class(mock_repo):
    resource = RepositoryResource(mock_repo, '/bar')
    with pytest.raises(RepositoryError) as e:
        resource.convert_to(NotARepoResource)  # noqa
        assert 'Unable to convert' in str(e.value)
