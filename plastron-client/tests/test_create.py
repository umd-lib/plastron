import re
from pathlib import Path
from uuid import uuid4

import pytest
from flask import url_for
from http_server_mock import HttpServerMock

from plastron.client import Client, Endpoint


@pytest.fixture
def client():
    return Client(endpoint=Endpoint(url='http://localhost:9999'))


@pytest.fixture
def repo_app():
    app = HttpServerMock(__name__, is_alive_route='/')
    app.config['RESOURCES'] = set()

    @app.route('/')
    def root():
        return 'Mock fcrepo server', 200

    @app.route('/<path:repo_path>', methods=['GET'])
    def get_resource(repo_path):
        if repo_path in app.config['RESOURCES']:
            return repo_path, 200
        else:
            return 'Not Found', 404

    @app.route('/<path:repo_path>', methods=['PUT'])
    def put_resource(repo_path):
        uri = url_for('get_resource', _external=True, repo_path=repo_path)
        app.config['RESOURCES'].add(repo_path)
        return uri, 201, {'Location': uri}

    @app.route('/<path:repo_path>', methods=['POST'])
    def post_resource(repo_path):
        uuid = str(uuid4())
        pairtree = [uuid[0:2], uuid[2:4], uuid[4:6], uuid[6:8]]
        path = str(Path(repo_path, *pairtree, uuid))
        app.config['RESOURCES'].add(path)
        uri = url_for('get_resource', _external=True, repo_path=path)
        return uri, 201, {'Location': uri}

    return app


def test_create_at_path(client: Client, repo_app):
    with repo_app.run('localhost', 9999):
        assert not client.path_exists('/foo')
        client.create_at_path(Path('/foo'))
        assert client.path_exists('/foo')


def test_create_at_path_nested(client: Client, repo_app):
    with repo_app.run('localhost', 9999):
        assert not client.path_exists('/foo')
        assert not client.path_exists('/foo/bar')
        assert not client.path_exists('/foo/bar/baz')
        client.create_at_path(Path('/foo/bar/baz'))
        assert client.path_exists('/foo')
        assert client.path_exists('/foo/bar')
        assert client.path_exists('/foo/bar/baz')


def test_create_in_container(client: Client, repo_app):
    with repo_app.run('localhost', 9999):
        client.create_at_path(Path('/foo'))
        assert client.path_exists('/foo')
        resource = client.create_in_container(Path('/foo'))
        assert resource.uri
        assert re.match('http://localhost:9999/foo/.+', str(resource.uri))
