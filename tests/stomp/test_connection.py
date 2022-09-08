from threading import Thread

import pytest
from coilmq.protocol import STOMP11
from coilmq.queue import QueueManager
from coilmq.server.socket_server import StompServer
from coilmq.store.memory import MemoryQueue
from coilmq.topic import TopicManager

from plastron.daemon import STOMPDaemon


@pytest.fixture()
def server_address():
    return 'localhost', 61613


@pytest.fixture()
def config(shared_datadir, server_address):
    return {
        'REPOSITORY': {},
        'MESSAGE_BROKER': {
            'SERVER': ':'.join(str(v) for v in server_address),
            'MESSAGE_STORE_DIR': str(shared_datadir / 'msg'),
            'DESTINATIONS': {
                'JOBS': '/queue/plastron.jobs',
                'JOB_PROGRESS': '/topic/plastron.jobs.progress',
                'JOB_STATUS': '/queue/plastron.jobs.status',
                'SYNCHRONOUS_JOBS': '/queue/plastron.jobs.synchronous',
                'REINDEXING': '/queue/reindex',
            }
        },
        'COMMANDS': {},
    }


@pytest.fixture()
def stomp_server(server_address):
    server = StompServer(
        server_address=server_address,
        queue_manager=QueueManager(store=MemoryQueue()),
        topic_manager=TopicManager(),
        protocol=STOMP11,
    )
    
    yield server

    server.server_close()


@pytest.fixture()
def plastrond_stomp(config):
    plastrond = STOMPDaemon(config=config)

    yield plastrond

    if plastrond.is_alive():
        plastrond.stopped.set()
        plastrond.stopped.wait()


def test_stomp_server_closed(stomp_server, plastrond_stomp):
    server_thread = Thread(target=stomp_server.serve_forever)
    server_thread.start()
    assert server_thread.is_alive()

    plastrond_stomp.start()
    assert plastrond_stomp.is_alive()
    # wait for startup and subscriptions to happen
    plastrond_stomp.started.wait()

    # stop the server
    stomp_server.shutdown()

    # wait for the daemon to shut down
    plastrond_stomp.stopped.wait(5)

    assert not plastrond_stomp.is_alive()
