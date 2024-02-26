import pytest
import stomp


@pytest.fixture
def stomp_server():
    return [('localhost', 61613)]


@pytest.fixture
def stomp_client(stomp_server):
    return stomp.Connection12(stomp_server)
