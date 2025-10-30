"""Common test fixtures for all Plastron packages"""

from os.path import abspath, dirname
from pathlib import Path

import pytest
import requests
from rdflib import URIRef

import plastron.validation.vocabularies


def pytest_configure(config):
    # Set VOCABULARIES_DIR and VOCABULARIES to point to local test files,
    # so that tests run without making a network call, and to provide greater
    # flexibility in defining the vocabularies used for testing.
    #
    # A network call will still be made if a vocabulary is not specified in the
    # "VOCABULARIES" dictionary, because of the fallback behavior in the
    # "get_vocabulary" method of
    # plastron-models/src/plastron/validation/vocabularies/__init__.py
    base_dir = Path(dirname(abspath(__file__)))
    plastron.validation.vocabularies.VOCABULARIES_DIR = base_dir / 'plastron-models/tests/validation/data/vocabularies'

    plastron.validation.vocabularies.VOCABULARIES = {
        URIRef('http://purl.org/dc/dcmitype/'): 'dcmitype.ttl',
        URIRef('http://vocab.lib.umd.edu/collection#'): 'collection.ttl',
        URIRef('http://vocab.lib.umd.edu/form#'): 'form.ttl',
        URIRef('http://vocab.lib.umd.edu/rightsStatement#'): 'rightsStatement.ttl',
        URIRef('http://vocab.lib.umd.edu/set#'): 'set.ttl',
        URIRef('http://vocab.lib.umd.edu/termsOfUse#'): 'termsOfUse.ttl'
    }


@pytest.fixture
def monkeypatch_request(monkeypatch):
    def _monkeypatch_request(response):
        if isinstance(response, type):
            response = response()
        monkeypatch.setattr(requests.Session, 'request', lambda *args, **kwargs: response)
    return _monkeypatch_request
