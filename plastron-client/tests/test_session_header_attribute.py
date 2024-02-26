from requests import Session

from plastron.client import SessionHeaderAttribute


class Foo:
    x_test_header = SessionHeaderAttribute('X-Header')

    def __init__(self):
        self.session = Session()


def test_session_header_attribute():
    # initially not set
    foo = Foo()
    assert 'X-Header' not in foo.session.headers

    # set the header
    foo.x_test_header = 'MyClient/1.0.0'
    assert foo.session.headers['X-Header'] == 'MyClient/1.0.0'

    # change the header
    foo.x_test_header = 'OtherAgent/2.0.0'
    assert foo.session.headers['X-Header'] == 'OtherAgent/2.0.0'

    # remove the header
    del foo.x_test_header
    assert 'X-Header' not in foo.session.headers


def test_delete_nonexistent_session_header():
    foo = Foo()
    del foo.x_test_header


def test_get_session_header():
    foo = Foo()
    foo.x_test_header = 'ABC'
    assert foo.x_test_header == 'ABC'
