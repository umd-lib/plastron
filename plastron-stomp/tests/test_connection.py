from threading import Thread

import pytest
from coilmq.protocol import STOMP11
from coilmq.queue import QueueManager
from coilmq.server.socket_server import StompServer
from coilmq.store.memory import MemoryQueue
from coilmq.topic import TopicManager

from plastron.messaging.broker import ServerTuple, Broker
from plastron.stomp.daemon import STOMPDaemon


@pytest.fixture()
def server_address() -> ServerTuple:
    return ServerTuple('localhost', 61613)


@pytest.fixture()
def broker(server_address, shared_datadir) -> Broker:
    return Broker(
        server=server_address,
        message_store_dir=(shared_datadir / 'msg'),
        destinations={
            'JOBS': '/queue/plastron.jobs',
            'JOB_PROGRESS': '/topic/plastron.jobs.progress',
            'JOB_STATUS': '/queue/plastron.jobs.status',
            'SYNCHRONOUS_JOBS': '/queue/plastron.jobs.synchronous',
            'REINDEXING': '/queue/reindex',
        }
    )


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
def plastrond_stomp(broker):
    plastrond = STOMPDaemon(broker=broker, repo_config={})

    yield plastrond

    if plastrond.is_alive():
        plastrond.stopped.set()
        plastrond.stopped.wait()


@pytest.mark.skip('intermittent failures, test is unreliable')
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
    plastrond_stomp.stopped.wait(10)

    assert not plastrond_stomp.is_alive()
