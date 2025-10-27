from requests import Session

from plastron.client import Client


def test_default_client_session(endpoint):
    client = Client(endpoint=endpoint)
    assert isinstance(client.session, Session)


class CustomSession(Session):
    pass


def test_custom_client_session(endpoint):
    session = CustomSession()
    client = Client(endpoint=endpoint, session=session)
    assert isinstance(client.session, CustomSession)
    assert client.session is session
