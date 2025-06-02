import pytest

from plastron.web import create_app


@pytest.fixture
def config_file_path(shared_datadir):
    return shared_datadir / 'plastron-config.yml'


@pytest.fixture
def app(config_file_path):
    return create_app(config_file_path)


@pytest.fixture
def app_client(app):
    return app.test_client()
