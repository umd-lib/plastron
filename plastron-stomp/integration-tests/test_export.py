import os
from pathlib import Path

import pytest


@pytest.mark.skipif(not os.environ.get('INTEGRATION_TESTS', False), reason='integration test')
def test_export(datadir, stomp_client):
    headers = {
        'PlastronJobId': 'test-export',
        'PlastronCommand': 'export',
        'PlastronArg-format': 'text/csv',
        'PlastronArg-export-binaries': 'true',
        'PlastronArg-output-dest': Path(os.getcwd()) / 'test-export.tar.gz',
    }
    body = os.environ['URI'] + '\n'
    stomp_client.connect()
    stomp_client.send('/queue/plastron.jobs', body=body, headers=headers)

    # TODO: verify the result of the export
