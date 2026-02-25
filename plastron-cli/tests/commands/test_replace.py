from argparse import Namespace
from unittest.mock import MagicMock

import pytest

from plastron.cli.commands.replace import replace
from plastron.context import PlastronContext
from plastron.models.pcdm import PCDMFile
from plastron.repo import Repository, RepositoryError, BinaryResource


def test_replace_repository_error():
    mock_resource = MagicMock(spec=BinaryResource)
    mock_resource.read.side_effect = RepositoryError
    mock_repo = MagicMock(spec=Repository)
    mock_repo.get_resource.return_value = mock_resource
    mock_context = MagicMock(spec=PlastronContext, repo=mock_repo)
    ctx = Namespace(obj=mock_context)
    with pytest.raises(RuntimeError):
        replace(ctx, '/foo', 'bar.jpg')


def test_replace_resource_does_not_exist():
    mock_resource = MagicMock(spec=BinaryResource, exists=False)
    mock_resource.read.return_value = mock_resource
    mock_repo = MagicMock(spec=Repository)
    mock_repo.get_resource.return_value = mock_resource
    mock_context = MagicMock(spec=PlastronContext, repo=mock_repo)
    ctx = Namespace(obj=mock_context)
    with pytest.raises(RuntimeError):
        replace(ctx, '/foo', 'bar.jpg')


def test_replace_resource_update_binary_failure():
    mock_resource = MagicMock(spec=BinaryResource, exists=False)
    mock_resource.read.return_value = mock_resource
    mock_resource.update_binary.side_effect = RepositoryError
    mock_repo = MagicMock(spec=Repository)
    mock_repo.get_resource.return_value = mock_resource
    mock_context = MagicMock(spec=PlastronContext, repo=mock_repo)
    ctx = Namespace(obj=mock_context)
    with pytest.raises(RuntimeError):
        replace(ctx, '/foo', 'bar.jpg')


def test_replace():
    file_obj = PCDMFile()
    mock_resource = MagicMock(spec=BinaryResource, exists=True)
    mock_resource.read.return_value = mock_resource
    mock_resource.describe.return_value = file_obj
    mock_repo = MagicMock(spec=Repository)
    mock_repo.get_resource.return_value = mock_resource
    mock_context = MagicMock(spec=PlastronContext, repo=mock_repo)
    ctx = Namespace(obj=mock_context)
    replace(ctx, '/foo', 'bar.jpg')
    mock_resource.update.assert_called_once()
    mock_resource.update_binary.assert_called_once()
    assert str(file_obj.title) == 'bar.jpg'
