import logging
import os
import threading
from contextlib import contextmanager
from http import HTTPStatus
from typing import Optional, Any

from rdflib import URIRef, Graph
from requests import ConnectionError, Response

from plastron.client.base import Client, ClientError
from plastron.client.endpoint import Endpoint
from plastron.client.utils import TypedText

logger = logging.getLogger(__name__)


@contextmanager
def transaction(client, keep_alive: int = 90):
    logger.info('Creating transaction')
    try:
        response = client.post(client.endpoint.transaction_endpoint)
    except ConnectionError as e:
        raise TransactionError(f'Failed to create transaction: {e}') from e
    if response.status_code == HTTPStatus.CREATED:
        txn_client = TransactionClient.from_client(client)
        txn_client.begin(uri=response.headers['Location'], keep_alive=keep_alive)
        logger.info(f'Created transaction at {txn_client.tx}')
        try:
            yield txn_client
        except ClientError:
            txn_client.rollback()
            raise
        else:
            txn_client.commit()
        finally:
            # when we leave the transaction context, always
            # set the stop flag on the keep-alive ping
            txn_client.tx.stop()
    else:
        raise TransactionError(f'Failed to create transaction: {response.status_code} {response.reason}')


class Transaction:
    def __init__(self, client: 'TransactionClient', uri: str, keep_alive: int = 90, active: bool = True):
        self.uri: str = uri
        self.keep_alive: TransactionKeepAlive = TransactionKeepAlive(client, keep_alive)
        self.active: bool = active
        if self.active:
            self.keep_alive.start()

    def __str__(self):
        return self.uri

    @property
    def maintenance_url(self):
        """Send a POST request to this URL to keep the transaction alive."""
        return os.path.join(self.uri, 'fcr:tx')

    @property
    def commit_url(self):
        """Send a POST request to this URL to commit the transaction."""
        return os.path.join(self.uri, 'fcr:tx/fcr:commit')

    @property
    def rollback_url(self):
        """Send a POST request to this URL to roll back the transaction."""
        return os.path.join(self.uri, 'fcr:tx/fcr:rollback')

    def stop(self):
        """
        Stop the keep-alive thread and set the `active` flag to `False`. This should
        always be called before committing or rolling back a transaction.
        """
        self.keep_alive.stop()
        self.active = False


class TransactionClient(Client):
    """HTTP client that transparently handles translating requests and responses
    sent during a Fedora transaction. Adds and removes the transaction identifier
    from URIs in graphs sent or returned. Adjusts the request URIs to include the
    transaction identifier."""

    @classmethod
    def from_client(cls, client: Client):
        """Build a `TransactionClient` from a regular `Client` object."""
        return cls(
            endpoint=client.endpoint,
            auth=client.session.auth,
            server_cert=client.session.verify,
            ua_string=client.ua_string,
            on_behalf_of=client.delegated_user,
            load_binaries=client.load_binaries,
        )

    def __init__(self, endpoint: Endpoint, **kwargs):
        super().__init__(endpoint, **kwargs)
        self.tx: Optional[Transaction] = None
        """The transaction"""

    def request(self, method: str, url: str, **kwargs) -> Response:
        """Makes sure the transaction keep-alive thread hasn't failed, and inserts the transaction
        id into the request URL. Then calls the `Client.request()` method with the same arguments.

        Raises a `RuntimeError` if the transaction keep-alive thread has failed."""
        if self.tx.keep_alive.failed.is_set():
            raise RuntimeError('Transaction keep-alive failed') from self.tx.keep_alive.exception

        request_url = str(self.insert_transaction_uri(URIRef(url)))
        return super().request(method, request_url, **kwargs)

    def get_location(self, response: Response) -> Optional[str]:
        """Removes the transaction id from the ``Location`` header returned by requests
        to create resources."""
        try:
            return str(self.remove_transaction_uri(URIRef(response.headers['Location'])))
        except KeyError:
            logger.warning('No Location header in response')
            return None

    def get_description(
        self,
        url: str,
        accept: str = 'application/n-triples',
        include_server_managed: bool = True,
    ) -> TypedText:
        text = super().get_description(
            url=str(self.insert_transaction_uri(URIRef(url))),
            accept=accept,
            include_server_managed=include_server_managed,
        )
        graph = self.remove_transaction_uri_for_graph(Graph().parse(data=text.value, format=text.media_type))
        return TypedText(text.media_type, graph.serialize(format=text.media_type))

    def put_graph(self, url, graph: Graph) -> Response:
        return super().put_graph(
            url=url,
            graph=self.insert_transaction_uri_for_graph(graph),
        )

    def patch_graph(self, url, deletes: Graph, inserts: Graph) -> Response:
        return super().patch_graph(
            url=url,
            deletes=self.insert_transaction_uri_for_graph(deletes),
            inserts=self.insert_transaction_uri_for_graph(inserts),
        )

    def get_description_uri(self, uri: str, response: Response = None) -> str:
        return str(self.remove_transaction_uri(URIRef(super().get_description_uri(uri=uri, response=response))))

    def insert_transaction_uri(self, uri: Any) -> Any:
        """If `uri` is in this client's `endpoint` but does not contain the current transaction ID,
        return a modified URI with the transaction ID added to it. Otherwise, return the `uri` argument
        as-is."""
        if not isinstance(uri, URIRef):
            return uri
        if uri.startswith(self.tx.uri):
            return uri
        if uri in self.endpoint:
            return URIRef(self.tx.uri + self.endpoint.repo_path(uri))
        return uri

    def remove_transaction_uri(self, uri: Any) -> Any:
        """If `uri` contains the current transaction ID, return a modified URI with the transaction ID
        removed. Otherwise, return the `uri` argument as-is."""
        if not isinstance(uri, URIRef):
            return uri
        if uri.startswith(self.tx.uri):
            return URIRef(uri.replace(self.tx.uri, self.endpoint.url))
        return uri

    def insert_transaction_uri_for_graph(self, graph: Optional[Graph]) -> Optional[Graph]:
        if graph is None:
            return None
        for s, p, o in graph:
            s_txn = self.insert_transaction_uri(s)
            o_txn = self.insert_transaction_uri(o)
            # swap the triple if either the subject or object is changed
            if s != s_txn or o != o_txn:
                graph.add((s_txn, p, o_txn))
                graph.remove((s, p, o))
        return graph

    def remove_transaction_uri_for_graph(self, graph: Optional[Graph]) -> Optional[Graph]:
        if graph is None:
            return None
        for s, p, o in graph:
            s_txn = self.remove_transaction_uri(s)
            o_txn = self.remove_transaction_uri(o)
            # swap the triple if either the subject or object is changed
            if s != s_txn or o != o_txn:
                graph.add((s_txn, p, o_txn))
                graph.remove((s, p, o))
        return graph

    def transaction(self, keep_alive: int = 90):
        """Immediately raises a `TransactionError`, since you cannot nest transactions."""
        raise TransactionError('Cannot nest transactions')

    @property
    def active(self):
        """Whether a transaction is set and active."""
        return self.tx and self.tx.active

    def begin(self, uri: str, keep_alive: int = 90):
        """Create a `Transaction` object and assign it to `tx`."""
        self.tx = Transaction(client=self, uri=uri, keep_alive=keep_alive)

    def maintain(self):
        """Sends an empty POST request to the `Transaction.maintenance_url` to keep it alive.
        Raises a `TransactionError` if the transaction is inactive, or there is a connection error
        or non-OK HTTP response from the repository server."""
        logger.info(f'Maintaining transaction {self}')
        if not self.active:
            raise TransactionError(f'Cannot maintain inactive transaction: {self.tx}')

        try:
            response = self.post(self.tx.maintenance_url)
        except ConnectionError as e:
            raise TransactionError(f'Failed to maintain transaction {self.tx}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Transaction {self} is active until {response.headers["Expires"]}')
        else:
            raise TransactionError(
                f'Failed to maintain transaction {self.tx}: {response.status_code} {response.reason}'
            )

    def commit(self):
        """Commits the transaction. Raises a `TransactionError` if the transaction is
        inactive, or there is a connection error or non-OK HTTP response from the repository
        server."""
        logger.info(f'Committing transaction {self.tx}')
        if not self.active:
            raise TransactionError(f'Cannot commit inactive transaction: {self.tx}')

        self.tx.stop()
        try:
            response = self.post(self.tx.commit_url)
        except ConnectionError as e:
            raise TransactionError(f'Failed to commit transaction {self.tx}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Committed transaction {self.tx}')
            return response
        else:
            raise TransactionError(
                f'Failed to commit transaction {self.tx}: {response.status_code} {response.reason}'
            )

    def rollback(self):
        """Rolls back the transaction. Raises a `TransactionError` if the transaction is
        inactive, or there is a connection error or non-OK HTTP response from the repository
        server."""
        logger.info(f'Rolling back transaction {self.tx}')
        if not self.tx.active:
            raise TransactionError(f'Cannot roll back inactive transaction: {self.tx}')

        self.tx.stop()
        try:
            response = self.post(self.tx.rollback_url)
        except ConnectionError as e:
            raise TransactionError(f'Failed to roll back transaction {self}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Rolled back transaction {self.tx}')
            return response
        else:
            raise TransactionError(
                f'Failed to roll back transaction {self.tx}: {response.status_code} {response.reason}'
            )


class TransactionKeepAlive(threading.Thread):
    """Thread to run in the background while a long-running transaction is being
    processed, to ensure that the transaction does not time out due to inactivity.
    Based on <https://stackoverflow.com/a/12435256/5124907>"""

    def __init__(self, txn_client: TransactionClient, interval: int):
        """Create a transaction keep-alive thread."""
        super().__init__(name='TransactionKeepAlive')
        self.txn_client: TransactionClient = txn_client
        """The transaction client."""

        self.interval: int = interval
        """Time between transaction maintenance requests."""

        self.stopped: threading.Event = threading.Event()
        """Flag indicating whether this transaction has been stopped."""

        self.failed: threading.Event = threading.Event()
        """Flag indicating whether this transaction has failed."""

        self.exception: Optional[TransactionError] = None
        """If this transaction could not be maintained, this holds the
        raised `TransactionError`."""

    def run(self):
        """Send a transaction maintenance request every `interval` seconds.
        If there is a `TransactionError` raised, set the `stopped` and `failed`
        flags on this thread, and store the raised exception as `exception`."""
        while not self.stopped.wait(self.interval):
            try:
                self.txn_client.maintain()
            except TransactionError as e:
                # stop trying to maintain the transaction
                self.stop()
                # set the "failed" flag to communicate back to the main thread
                # that we were unable to maintain the transaction
                self.exception = e
                self.failed.set()

    def stop(self):
        """Set the `stopped` flag on this thread."""
        self.stopped.set()


class TransactionError(Exception):
    """Raised when a transaction fails."""
    pass
