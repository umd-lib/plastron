import dataclasses
from argparse import Namespace
from random import randint
from unittest.mock import MagicMock

import pytest

from plastron.cli.commands.publish import Command, publish
from plastron.client import Endpoint, Client, TypedText
from plastron.context import PlastronContext
from plastron.handles import HandleInfo, HandleServerError
from plastron.models.umd import Item
from plastron.namespaces import umdaccess
from plastron.repo import Repository, RepositoryError
from plastron.repo.publish import PublishableResource


@pytest.fixture
def handle():
    return HandleInfo(
        exists=True,
        prefix='1903.1',
        suffix='123',
        url='http://digital-local/foo',
    )


class MockHandleClient:
    default_repo = 'fcrepo'

    GET_HANDLE_LOOKUP = {
        # existing handle with fcrepo target URL
        '1903.1/123': HandleInfo(
            exists=True,
            prefix='1903.1',
            suffix='123',
            url='http://digital-local/foo',
        ),
        # existing handle with fedora2 target URL
        # if publishing the resource with this handle,
        # should call the handle server to update the
        # target URL
        '1903.1/456': HandleInfo(
            exists=True,
            prefix='1903.1',
            suffix='456',
            url='http://fedora2-local/bar',
        ),
        # there is no handle with this prefix/suffix pair
        '1903.1/789': HandleInfo(
            exists=False,
        ),
    }
    FIND_HANDLE_LOOKUP = {
        # this fcrepo resource has a handle and its target
        # URL points to the correct public URL
        'http://fcrepo-local:8080/fcrepo/rest/foo': HandleInfo(
            exists=True,
            prefix='1903.1',
            suffix='123',
            url='http://digital-local/foo',
        ),
        # this fcrepo resource has a handle and its target
        # URL needs to be updated to the correct public URL
        'http://fcrepo-local:8080/fcrepo/rest/bar': HandleInfo(
            exists=True,
            prefix='1903.1',
            suffix='456',
            url='http://fedora2-local/bar',
        ),
    }

    def get_info(self, prefix: str, suffix: str) -> HandleInfo:
        return self.GET_HANDLE_LOOKUP.get(f'{prefix}/{suffix}', HandleInfo(exists=False))

    def find_handle(self, repo_id: str, _repo: str = None) -> HandleInfo:
        return self.FIND_HANDLE_LOOKUP.get(repo_id, HandleInfo(exists=False))

    @staticmethod
    def create_handle(repo_id: str, url: str, prefix: str = None, _repo: str = None) -> HandleInfo:
        if repo_id.endswith('NO_HANDLE'):
            raise HandleServerError('no handle')
        return HandleInfo(exists=True, prefix=prefix, suffix=str(randint(1000, 10000)), url=url)

    @staticmethod
    def update_handle(handle: HandleInfo, **fields) -> HandleInfo:
        return dataclasses.replace(handle, **fields)


def get_mock_context(obj, path):
    endpoint = Endpoint('http://fcrepo-local:8080/fcrepo/rest')
    mock_client = MagicMock(spec=Client, endpoint=endpoint)
    mock_client.get_description.return_value = TypedText('application/n-triples', '')
    mock_repo = MagicMock(spec=Repository, client=mock_client, endpoint=endpoint)
    resource = PublishableResource(repo=mock_repo, path=path)
    resource.describe = lambda _: obj
    mock_repo.__getitem__.return_value = resource
    return MagicMock(
        spec=PlastronContext,
        repo=mock_repo,
        handle_client=MockHandleClient(),
        get_public_url=lambda res: res.url.replace('fcrepo-local:8080/fcrepo/rest', 'digital-local')
    )


def test_publish(handle):
    obj = Item()
    publish(Namespace(obj=get_mock_context(obj, '/foo')), uris=['http://fcrepo-local:8080/fcrepo/rest/foo'])

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values


def test_publish_hidden(handle):
    obj = Item()
    publish(
        Namespace(obj=get_mock_context(obj, '/foo')),
        force_hidden=True,
        uris=['http://fcrepo-local:8080/fcrepo/rest/foo'],
    )

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden in obj.rdf_type.values


def test_publish_force_visible(handle):
    obj = Item(rdf_type=umdaccess.Hidden)
    publish(
        Namespace(obj=get_mock_context(obj, '/foo')),
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


def test_publish_command_class(handle):
    obj = Item()
    cmd = Command(get_mock_context(obj, '/foo'))
    cmd(Namespace(uris=['http://fcrepo-local:8080/fcrepo/rest/foo'], hidden=False, visible=False))

    assert umdaccess.Published in obj.rdf_type.values
    assert umdaccess.Hidden not in obj.rdf_type.values
