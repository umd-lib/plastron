import re
from pathlib import Path
from uuid import uuid4

import pytest
from flask import url_for
from http_server_mock import HttpServerMock

from plastron.commands import create
from plastron.http import Repository


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


@pytest.fixture
def repo_base_config():
    """Required parameters for Repository configuration"""
    return {
        'REST_ENDPOINT': 'http://localhost:9999',
        'RELPATH': '/pcdm',
        'LOG_DIR': '/logs',
        'AUTH_TOKEN': 'foobar'
    }


def test_create_at_path(repo_base_config, repo_app):
    cmd = create.Command()
    cmd.repo = Repository(repo_base_config)
    with repo_app.run('localhost', 9999):
        assert not cmd.repo.path_exists('/foo')
        cmd.create_at_path(Path('/foo'))
        assert cmd.repo.path_exists('/foo')


def test_create_at_path_nested(repo_base_config, repo_app):
    cmd = create.Command()
    cmd.repo = Repository(repo_base_config)
    with repo_app.run('localhost', 9999):
        assert not cmd.repo.path_exists('/foo')
        assert not cmd.repo.path_exists('/foo/bar')
        assert not cmd.repo.path_exists('/foo/bar/baz')
        cmd.create_at_path(Path('/foo/bar/baz'))
        assert cmd.repo.path_exists('/foo')
        assert cmd.repo.path_exists('/foo/bar')
        assert cmd.repo.path_exists('/foo/bar/baz')


def test_create_in_container(repo_base_config, repo_app):
    cmd = create.Command()
    cmd.repo = Repository(repo_base_config)
    with repo_app.run('localhost', 9999):
        cmd.create_at_path(Path('/foo'))
        assert cmd.repo.path_exists('/foo')
        resource = cmd.create_in_container(Path('/foo'))
        assert resource.uri
        assert re.match('http://localhost:9999/foo/.+', str(resource.uri))
