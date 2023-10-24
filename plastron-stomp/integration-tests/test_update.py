import json
import os

import pytest


@pytest.mark.skipif(not os.environ.get('INTEGRATION_TESTS', False), reason='integration test')
def test_update(stomp_client):
    headers = {
        'PlastronJobId': 'test-update',
        'PlastronCommand': 'update',
    }
    body = json.dumps({
        'uris': [os.environ['URI']],
        'sparql_update': 'INSERT DATA { <> <http://purl.org/dc/terms/title> "Moonpig" }'
    })
    stomp_client.connect()
    stomp_client.send('/queue/plastron.jobs', headers=headers, body=body)

    # TODO: verify the result of the update
