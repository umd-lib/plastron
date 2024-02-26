import os

import pytest


@pytest.mark.skipif(not os.environ.get('INTEGRATION_TESTS', False), reason='integration test')
def test_echo(stomp_client):
    headers = {
        'PlastronJobId': 'test-echo',
        'PlastronCommand': 'echo',
    }
    body = 'Hello world!\n'
    stomp_client.connect()
    stomp_client.send('/queue/plastron.jobs', headers=headers, body=body)

    # TODO: verify the result of the echo
