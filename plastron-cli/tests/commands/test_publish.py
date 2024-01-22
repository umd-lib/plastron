from argparse import Namespace
from unittest.mock import MagicMock

from plastron.cli.commands.publish import Command, get_publication_status, publish
from plastron.cli.context import PlastronContext
from plastron.handles import Handle, HandleServiceClient
from plastron.models.umd import Item
from plastron.namespaces import umdaccess, umdtype
from plastron.rdfmapping.resources import RDFResource
from plastron.repo import Repository, RepositoryError, RepositoryResource
import pytest
from rdflib import Literal


def get_mock_context(obj):
    mock_resource = MagicMock(spec=RepositoryResource)
    mock_resource.read.return_value = mock_resource
    mock_resource.describe.return_value = obj
    mock_resource.update = lambda: obj.apply_changes()
    mock_repo = MagicMock(spec=Repository)
    mock_repo.__getitem__.return_value = mock_resource
    mock_handle_client = MagicMock(spec=HandleServiceClient)
    mock_handle_client.get_handle.return_value = None
    mock_handle_client.create_handle.return_value = Handle(
        prefix='1903.1',
        suffix='123',
        url='http://example.com/foobar',
    )
    return MagicMock(
        spec=PlastronContext,
        repo=mock_repo,
        handle_client=mock_handle_client,
        get_public_url=lambda uri: uri.replace('fcrepo-local:8080/fcrepo/rest', 'digital-local')
    )


@pytest.mark.parametrize(
    ('obj', 'expected_status'),
    [
        (RDFResource(), 'Unpublished'),
        (RDFResource(rdf_type=umdaccess.Hidden), 'UnpublishedHidden'),
        (RDFResource(rdf_type=umdaccess.Published), 'Published'),
        (RDFResource(rdf_type=[umdaccess.Published, umdaccess.Hidden]), 'PublishedHidden'),
    ],
)
def test_get_publication_status(obj, expected_status):
    assert get_publication_status(obj) == expected_status


def test_publish():
    obj = Item()
    publish(Namespace(obj=get_mock_context(obj)), uris=['http://fcrepo-local:8080/fcrepo/rest/foo'])

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values


def test_publish_hidden():
    obj = Item()
    publish(Namespace(obj=get_mock_context(obj)), force_hidden=True, uris=['http://fcrepo-local:8080/fcrepo/rest/foo'])

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden in obj.rdf_type.values


def test_publish_force_visible():
    obj = Item(rdf_type=umdaccess.Hidden)
    publish(Namespace(obj=get_mock_context(obj)), force_visible=True, uris=['http://fcrepo-local:8080/fcrepo/rest/foo'])

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values


def test_publish_repo_error(caplog):
    mock_repo = MagicMock(spec=Repository)
    mock_repo.__getitem__.side_effect = RepositoryError('something bad')

    publish(
        Namespace(obj=MagicMock(spec=PlastronContext, repo=mock_repo)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )
    assert 'Unable to retrieve http://fcrepo-local:8080/fcrepo/rest/foo: something bad' in caplog.messages


def test_publish_handle_service_error(caplog):
    obj = Item()
    mock_resource = MagicMock(spec=RepositoryResource)
    mock_resource.read.return_value = mock_resource
    mock_resource.describe.return_value = obj
    mock_resource.update = lambda: obj.apply_changes()
    mock_repo = MagicMock(spec=Repository)
    mock_repo.__getitem__.return_value = mock_resource
    mock_handle_client = MagicMock(spec=HandleServiceClient)
    mock_handle_client.get_handle.return_value = None
    mock_handle_client.create_handle.return_value = None

    publish(
        Namespace(obj=MagicMock(spec=PlastronContext, repo=mock_repo, handle_client=mock_handle_client)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )
    assert 'Unable to find or create handle for http://fcrepo-local:8080/fcrepo/rest/foo' in caplog.messages


def test_publish_new_handle_update_handle_in_repo():
    obj = Item(handle='hdl:1903.1/987')
    publish(Namespace(obj=get_mock_context(obj)), uris=['http://fcrepo-local:8080/fcrepo/rest/foo'])

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values
    assert obj.handle.value == Literal('hdl:1903.1/123', datatype=umdtype.handle)


def test_publish_existing_handle_update_handle_in_repo():
    obj = Item(handle='hdl:1903.1/987')
    handle = Handle(
        prefix='1903.1',
        suffix='123',
        url='http://example.com/foobar',
    )
    mock_resource = MagicMock(spec=RepositoryResource)
    mock_resource.read.return_value = mock_resource
    mock_resource.describe.return_value = obj
    mock_resource.update = lambda: obj.apply_changes()
    mock_repo = MagicMock(spec=Repository)
    mock_repo.__getitem__.return_value = mock_resource
    mock_handle_client = MagicMock(spec=HandleServiceClient)
    mock_handle_client.get_handle.return_value = handle
    mock_handle_client.update_handle.return_value = handle

    publish(
        Namespace(obj=MagicMock(spec=PlastronContext, repo=mock_repo, handle_client=mock_handle_client)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values
    assert obj.handle.value == Literal('hdl:1903.1/123', datatype=umdtype.handle)


def test_publish_command_class():
    obj = Item()
    cmd = Command(get_mock_context(obj))
    cmd(Namespace(uris=['http://fcrepo-local:8080/fcrepo/rest/foo'], hidden=False, visible=False))

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values
