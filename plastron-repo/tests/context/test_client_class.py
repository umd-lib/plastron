import pytest

from plastron.client import Client
from plastron.client.proxied import ProxiedClient
from plastron.context import PlastronContext


@pytest.mark.parametrize(
    ('repo_config', 'expected_class'),
    [
        (
            {
                'REST_ENDPOINT': 'https://fcrepo.lib.umd.edu/fcrepo/rest',
            },
            Client,
        ),
        (
            {
                'REST_ENDPOINT': 'https://fcrepo.lib.umd.edu/fcrepo/rest',
                'ORIGIN': 'http://fcrepo-webapp:8080/fcrepo/rest',
            },
            ProxiedClient,
        ),
    ]
)
def test_client_class(repo_config, expected_class):
    config = {'REPOSITORY': repo_config}
    context = PlastronContext(config)
    assert isinstance(context.client, expected_class)
