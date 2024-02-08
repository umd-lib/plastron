import os

import pytest
import requests


@pytest.mark.skipif(not os.environ.get('INTEGRATION_TESTS', False), reason='integration test')
def test_unpublish_http(jsonld_context, inbox_url):
    target_uri = os.environ['URI']
    response = requests.post(
        url=inbox_url,
        json={
            '@context': jsonld_context,
            "type": "Unpublish",
            "object": [target_uri]
        },
        headers={
            'Content-Type': 'application/json',
        }
    )
    assert response.ok
