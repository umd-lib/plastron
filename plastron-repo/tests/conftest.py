"""Common test fixtures for all Plastron packages"""

import pytest
import requests


@pytest.fixture
def monkeypatch_request(monkeypatch):
    def _monkeypatch_request(response):
        if isinstance(response, type):
            response = response()
        monkeypatch.setattr(requests.Session, 'request', lambda *args, **kwargs: response)

    return _monkeypatch_request
