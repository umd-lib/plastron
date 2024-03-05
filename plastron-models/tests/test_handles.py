from http import HTTPStatus
import json
import httpretty
import pytest
from rdflib import Literal

from plastron.handles import Handle, HandleBearingResource, HandleServerError, HandleServiceClient
from plastron.namespaces import umdtype


@pytest.fixture
def handle():
    return Handle(prefix='1903.1', suffix='123', url='http://example.com/foobar')


def test_handle_attributes(handle):
    assert handle.prefix == '1903.1'
    assert handle.suffix == '123'
    assert handle.url == 'http://example.com/foobar'
    assert handle.hdl_uri == 'hdl:1903.1/123'
    assert str(handle) == '1903.1/123'


def test_handle_bearing_resource():
    obj = HandleBearingResource(handle=Literal('hdl:1903.1/123', datatype=umdtype.handle))
    obj.apply_changes()
    assert obj.is_valid
    assert obj.handle.value == Literal('hdl:1903.1/123', datatype=umdtype.handle)


def test_invalid_handle_bearing_resource():
    obj = HandleBearingResource(handle=Literal('not-a-handle', datatype=umdtype.handle))
    assert not obj.is_valid
    assert obj.validate()['handle'].message == 'is not a handle URI'


@pytest.fixture
def handle_client():
    return HandleServiceClient('http://handle-local:3000', jwt_token='TOKEN')


@httpretty.activate
def test_get_handle_error(handle_client):
    httpretty.register_uri(
        httpretty.GET,
        uri='http://handle-local:3000/handles/exists',
        status=HTTPStatus.BAD_REQUEST,
    )
    with pytest.raises(HandleServerError):
        handle_client.get_handle('http://example.com/foobar')


@httpretty.activate
def test_get_handle_not_found(handle_client):
    httpretty.register_uri(
        httpretty.GET,
        uri='http://handle-local:3000/handles/exists',
        status=HTTPStatus.NOT_FOUND,
    )
    assert handle_client.get_handle('http://example.com/foobar') is None


@httpretty.activate
def test_get_handle_does_not_exist(handle_client):
    httpretty.register_uri(
        httpretty.GET,
        uri='http://handle-local:3000/handles/exists',
        body=json.dumps({'exists': False})
    )
    assert handle_client.get_handle('http://example.com/foobar') is None


@httpretty.activate
def test_get_handle_exists(handle_client):
    httpretty.register_uri(
        httpretty.GET,
        uri='http://handle-local:3000/handles/exists',
        body=json.dumps({'exists': True, 'prefix': '1903.1', 'suffix': '123', 'url': 'http://example.com/foobar'})
    )
    handle = handle_client.get_handle('http://example.com/foobar')
    assert handle.prefix == '1903.1'
    assert handle.suffix == '123'
    assert handle.url == 'http://example.com/foobar'


@httpretty.activate
def test_create_handle_success(handle_client):
    httpretty.register_uri(
        httpretty.POST,
        uri='http://handle-local:3000/handles',
        body=json.dumps(
            {'suffix': '123', 'request': {'url': 'http://example.com/foobar', 'prefix': '1903.1'}}
        )
    )
    handle = handle_client.create_handle(repo_uri='http://localhost/fcrepo/foobar', url='http://example.com/foobar')
    assert handle.prefix == '1903.1'
    assert handle.suffix == '123'
    assert handle.url == 'http://example.com/foobar'


@httpretty.activate
def test_create_handle_error(handle_client):
    httpretty.register_uri(
        httpretty.POST,
        uri='http://handle-local:3000/handles',
        status=HTTPStatus.BAD_REQUEST,
    )
    with pytest.raises(HandleServerError):
        handle_client.create_handle(repo_uri='http://localhost/fcrepo/foobar', url='http://example.com/foobar')


@httpretty.activate
def test_update_handle(handle, handle_client):
    httpretty.register_uri(
        httpretty.PATCH,
        uri='http://handle-local:3000/handles/1903.1/123',
    )
    updated_handle = handle_client.update_handle(handle=handle, url='http://example.com/new-url')
    assert updated_handle.url == 'http://example.com/new-url'


@httpretty.activate
def test_update_handle_error(handle, handle_client):
    httpretty.register_uri(
        httpretty.PATCH,
        uri='http://handle-local:3000/handles/1903.1/123',
        status=HTTPStatus.BAD_REQUEST,
    )
    with pytest.raises(HandleServerError):
        handle_client.update_handle(handle=handle, url='http://example.com/new-url')
