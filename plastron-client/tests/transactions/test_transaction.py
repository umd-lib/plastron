from http import HTTPStatus
from unittest.mock import Mock

import pytest
from requests import Response

from plastron.client.transactions import (
    Fedora4TransactionClient,
    get_transaction_client_class,
    Fedora6TransactionClient,
)
from plastron.client.utils import fedora_tx


@pytest.fixture
def fedora4_response():
    return Mock(
        spec=Response,
        status_code=HTTPStatus.CREATED,
        headers={'Location': 'http://example.com/fcrepo/rest/tx:6b6f1720-6872-47ad-912e-d567a90286f6'},
        links={},
    )


@pytest.fixture
def fedora6_response():
    return Mock(
        spec=Response,
        status_code=HTTPStatus.CREATED,
        headers={'Location': 'http://example.com/fcrepo/rest/fcr:tx/6b6f1720-6872-47ad-912e-d567a90286f6'},
        links={
            str(fedora_tx.commitEndpoint): {
                'rel': str(fedora_tx.commitEndpoint),
                'url': 'http://example.com/fcrepo/rest/fcr:tx/6b6f1720-6872-47ad-912e-d567a90286f6',
            }
        },
    )


def test_get_fedora4_transaction_client_class(fedora4_response):
    assert get_transaction_client_class(fedora4_response) is Fedora4TransactionClient


def test_get_fedora6_transaction_client_class(fedora6_response):
    assert get_transaction_client_class(fedora6_response) is Fedora6TransactionClient
