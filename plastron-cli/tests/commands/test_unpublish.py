from argparse import Namespace
from unittest.mock import MagicMock

from plastron.cli.commands.unpublish import Command, unpublish
from plastron.cli.context import PlastronContext
from plastron.models.umd import Item
from plastron.namespaces import umdaccess
from plastron.repo import Repository, RepositoryError, RepositoryResource


def get_mock_context(obj):
    mock_resource = MagicMock(spec=RepositoryResource)
    mock_resource.read.return_value = mock_resource
    mock_resource.describe.return_value = obj
    mock_resource.update = lambda: obj.apply_changes()
    mock_repo = MagicMock(spec=Repository)
    mock_repo.__getitem__.return_value = mock_resource

    return MagicMock(spec=PlastronContext, repo=mock_repo)


def test_unpublish():
    obj = Item(rdf_type=umdaccess.Published)

    unpublish(
        Namespace(obj=get_mock_context(obj)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )

    assert umdaccess.Published not in obj.rdf_type.values


def test_unpublish_hidden():
    obj = Item(rdf_type=umdaccess.Published)

    unpublish(
        Namespace(obj=get_mock_context(obj)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
        force_hidden=True,
    )

    assert umdaccess.Published not in obj.rdf_type.values
    assert umdaccess.Hidden in obj.rdf_type.values


def test_unpublish_make_visible():
    obj = Item(rdf_type=[umdaccess.Published, umdaccess.Hidden])

    unpublish(
        Namespace(obj=get_mock_context(obj)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
        force_visible=True,
    )

    assert umdaccess.Published not in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values


def test_unpublish_repo_error(caplog):
    mock_repo = MagicMock(spec=Repository)
    mock_repo.__getitem__.side_effect = RepositoryError('something bad')

    unpublish(
        Namespace(obj=MagicMock(spec=PlastronContext, repo=mock_repo)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )
    assert 'Unable to retrieve http://fcrepo-local:8080/fcrepo/rest/foo: something bad' in caplog.messages


def test_unpublish_command_class():
    obj = Item(rdf_type=umdaccess.Published)
    cmd = Command(get_mock_context(obj))
    cmd(Namespace(uris=['http://fcrepo-local:8080/fcrepo/rest/foo'], hidden=False, visible=False))

    assert umdaccess.Published not in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values
