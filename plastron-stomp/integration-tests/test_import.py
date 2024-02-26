import os

import pytest

from plastron.utils import datetimestamp


@pytest.mark.skipif(not os.environ.get('INTEGRATION_TESTS', False), reason='integration test')
def test_import(datadir, stomp_client):
    headers = {
        'PlastronJobId': f'test-import-{datetimestamp()}',
        'PlastronCommand': 'import',
        'PlastronArg-model': 'Item',
        'PlastronArg-container': '/',
        'PlastronArg-binaries-location': str(datadir / 'import-files'),
        'PlastronArg-limit': '1',
    }
    body = (datadir / 'import.csv').read_text()
    stomp_client.connect()
    stomp_client.send('/queue/plastron.jobs', headers=headers, body=body)

    # TODO: verify the result of the import
