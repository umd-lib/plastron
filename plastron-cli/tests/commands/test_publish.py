from argparse import Namespace
from unittest.mock import MagicMock

import pytest
from rdflib import Literal

from plastron.cli.commands.publish import Command, publish
from plastron.context import PlastronContext
from plastron.client import Endpoint, Client, TypedText
from plastron.handles import Handle, HandleServiceClient
from plastron.models.umd import Item
from plastron.namespaces import umdaccess, umdtype
from plastron.rdfmapping.resources import RDFResource
from plastron.repo import Repository, RepositoryError
from plastron.repo.publish import get_publication_status, PublishableResource


@pytest.fixture
def handle():
    return Handle(
        prefix='1903.1',
        suffix='123',
        url='http://fcrepo-local:8080/fcrepo/rest/foo',
    )


def get_mock_context(obj, handle, existing_handle=None):
    endpoint = Endpoint('http://fcrepo-local:8080/fcrepo/rest')
    mock_client = MagicMock(spec=Client, endpoint=endpoint)
    mock_client.get_description.return_value = TypedText('application/n-triples', '')
    mock_repo = MagicMock(spec=Repository, client=mock_client, endpoint=endpoint)
    resource = PublishableResource(repo=mock_repo, path='/foo')
    resource.describe = lambda _: obj
    mock_repo.__getitem__.return_value = resource
    mock_handle_client = MagicMock(spec=HandleServiceClient)
    mock_handle_client.get_handle.return_value = existing_handle
    mock_handle_client.create_handle.return_value = handle
    mock_handle_client.update_handle.return_value = handle
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


def test_publish(handle):
    obj = Item()
    publish(Namespace(obj=get_mock_context(obj, handle)), uris=['http://fcrepo-local:8080/fcrepo/rest/foo'])

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values


def test_publish_hidden(handle):
    obj = Item()
    publish(
        Namespace(obj=get_mock_context(obj, handle)),
        force_hidden=True,
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden in obj.rdf_type.values


def test_publish_force_visible(handle):
    obj = Item(rdf_type=umdaccess.Hidden)
    publish(
        Namespace(obj=get_mock_context(obj, handle)),
        force_visible=True,
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )

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

    publish(
        Namespace(obj=get_mock_context(obj, handle=None)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )
    assert 'Unable to find or create handle for http://fcrepo-local:8080/fcrepo/rest/foo' in caplog.messages


def test_publish_new_handle_update_handle_in_repo(handle):
    obj = Item(handle=Literal('hdl:1903.1/987'))
    publish(Namespace(obj=get_mock_context(obj, handle)), uris=['http://fcrepo-local:8080/fcrepo/rest/foo'])

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values
    assert obj.handle.value == Literal('hdl:1903.1/123', datatype=umdtype.handle)


def test_publish_existing_handle_update_handle_in_repo(handle):
    obj = Item(handle=Literal('hdl:1903.1/987'))

    publish(
        Namespace(obj=get_mock_context(obj, handle, existing_handle=handle)),
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values
    assert obj.handle.value == Literal('hdl:1903.1/123', datatype=umdtype.handle)


def test_publish_command_class(handle):
    obj = Item()
    cmd = Command(get_mock_context(obj, handle))
    cmd(Namespace(uris=['http://fcrepo-local:8080/fcrepo/rest/foo'], hidden=False, visible=False))

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values
