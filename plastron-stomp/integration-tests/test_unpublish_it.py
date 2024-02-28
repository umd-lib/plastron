import os

import pytest


@pytest.mark.skipif(not os.environ.get('INTEGRATION_TESTS', False), reason='integration test')
def test_unpublish(datadir, stomp_client):
    headers = {
        'PlastronJobId': 'test-unpublish',
        'PlastronCommand': 'unpublish',
        'PlastronArg-force-hidden': 'false',
        'PlastronArg-force-visible': 'false',
    }
    body = os.environ['URI'] + '\n'
    stomp_client.connect()
    stomp_client.send('/queue/plastron.jobs', body=body, headers=headers)

    # TODO: verify the result of the export
