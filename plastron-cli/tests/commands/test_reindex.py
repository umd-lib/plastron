from tempfile import TemporaryDirectory

import pytest

from plastron.cli.commands.reindex import Command
from plastron.messaging.broker import Broker, ServerTuple
from plastron.repo import RepositoryResource, Tombstone


@pytest.fixture
def broker():
    tmp_dir = TemporaryDirectory()
    return Broker(
        server=ServerTuple('localhost', 61613),
        message_store_dir=tmp_dir.name,
        destinations={
            'indexing': '/queue/indexing',
            'reindexing': '/queue/reindexing',
        },
    )


def test_create_message(plastron_context, broker):
    plastron_context.broker = broker
    resource = RepositoryResource(plastron_context.repo, '/foo')
    expected_headers = {
        'FcrepoUri': str(resource.url),
        'FcrepoPath': '/foo',
        'FcrepoUser': 'plastron',
    }
    cmd = Command(plastron_context)
    destination, message = cmd.create_message(resource, username='plastron', routing_headers={})
    assert destination.name == '/queue/reindexing'
    assert not any(header.startswith('Camel') for header in message.headers.keys())
    for k, v in expected_headers.items():
        assert message.headers[k] == v


def test_create_message_for_tombstone(plastron_context, broker):
    plastron_context.broker = broker
    resource = Tombstone(RepositoryResource(plastron_context.repo, '/foo'))
    expected_headers = {
        'FcrepoEventName': 'delete',
        'FcrepoUri': str(resource.url),
        'FcrepoPath': '/foo',
        'FcrepoUser': 'plastron',
    }
    cmd = Command(plastron_context)
    destination, message = cmd.create_message(resource, username='plastron', routing_headers={})
    assert destination.name == '/queue/indexing'
    assert not any(header.startswith('Camel') for header in message.headers.keys())
    for k, v in expected_headers.items():
        assert message.headers[k] == v
