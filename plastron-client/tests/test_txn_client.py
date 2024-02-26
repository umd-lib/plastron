import pytest
from rdflib import URIRef, Literal

from plastron.client import Endpoint, TransactionClient, Transaction, TransactionError


@pytest.fixture()
def endpoint():
    return Endpoint(url='http://example.com/repo')


@pytest.fixture()
def txn_client(endpoint):
    txn_client = TransactionClient(endpoint=endpoint)
    txn_client.tx = Transaction(client=txn_client, uri='http://example.com/repo/tx:123456')
    yield txn_client
    # after the tests are done, make sure the keep-alive thread stops
    txn_client.tx.stop()


@pytest.mark.parametrize(
    ('value', 'expected_uri'),
    [
        (URIRef('http://example.com/repo/tx:123456/foo'), URIRef('http://example.com/repo/foo')),
        # do nothing if it is already removed
        (URIRef('http://example.com/repo/foo'), URIRef('http://example.com/repo/foo')),
        # do nothing if it is not a URIRef
        ('bar', 'bar'),
        (Literal('bar'), Literal('bar')),
    ]
)
def test_remove_transaction_uri(txn_client, value, expected_uri):
    assert txn_client.remove_transaction_uri(value) == expected_uri


@pytest.mark.parametrize(
    ('value', 'expected_uri'),
    [
        (URIRef('http://example.com/repo/foo'), URIRef('http://example.com/repo/tx:123456/foo')),
        # do nothing if it is already inserted
        (URIRef('http://example.com/repo/tx:123456/foo'), URIRef('http://example.com/repo/tx:123456/foo')),
        # do nothing if it is not part of the endpoint (e.g., an external vocabulary value)
        (URIRef('http://pcdm.org/model#File'), URIRef('http://pcdm.org/model#File')),
        # do nothing if it is not a URIRef
        ('bar', 'bar'),
        (Literal('bar'), Literal('bar')),
    ]
)
def test_insert_transaction_uri(txn_client, value, expected_uri):
    assert txn_client.insert_transaction_uri(value) == expected_uri


def test_cannot_nest_transactions(txn_client):
    with pytest.raises(TransactionError) as e:
        txn_client.transaction()

    assert str(e.value) == 'Cannot nest transactions'
