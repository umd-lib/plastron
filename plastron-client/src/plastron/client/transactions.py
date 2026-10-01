import logging
import os
import threading
from abc import abstractmethod, ABC
from contextlib import contextmanager
from http import HTTPStatus
from typing import Any, Generator

from rdflib import Graph, URIRef
from requests import ConnectionError, Response

from plastron.client.base import Client, ClientError
from plastron.client.utils import TypedText, fedora_tx

logger = logging.getLogger(__name__)


def get_transaction_client_class(response: Response) -> type['TransactionClientBase']:
    """Guess the transaction type based on the response from the Fedora server, and return
    the appropriate transaction client class."""
    if str(fedora_tx.commitEndpoint) in response.links:
        # the presence of a Link header indicates this is a Fedora 6+ style transaction
        # Location: .../rest/fcr:tx/{uuid}
        return Fedora6TransactionClient
    else:
        # otherwise, assume it is a Fedora 4 style transaction
        # Location: .../rest/tx:{uuid}
        return Fedora4TransactionClient


@contextmanager
def transaction(client: Client, keep_alive: int = 90) -> Generator['TransactionClientBase', Any, None]:
    """Context manager for using transactions. The transaction is created by this function,
    and is automatically committed when leaving this context. If an exception is raised in
    the block, it instead attempts to roll back the transaction."""
    logger.info('Creating transaction')
    try:
        response = client.post(client.transaction_endpoint)
    except ConnectionError as e:
        raise TransactionError(f'Failed to create transaction: {e}') from e
    if response.status_code == HTTPStatus.CREATED:
        txn_client = get_transaction_client_class(response).from_client(
            client=client,
            tx_uri=response.headers['Location'],
            keep_alive=keep_alive,
        )

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
    """A single transaction."""

    def __init__(self, client: 'TransactionClientBase', uri: str, keep_alive: int = 90, active: bool = True):
        self.uri: str = uri
        """The URI of the transaction."""

        self.keep_alive: TransactionKeepAlive = TransactionKeepAlive(client, keep_alive)
        """Keep-alive thread. Default interval between keep-alive requests is 90 seconds."""

        self.active: bool = active
        """Whether this transaction is active. Defaults to `true`."""

        if self.active:
            self.keep_alive.start()

    def __str__(self):
        return self.uri

    def stop(self):
        """
        Stop the keep-alive thread and set the `active` flag to `False`. This should
        always be called before committing or rolling back a transaction.
        """
        self.keep_alive.stop()
        self.active = False


class TransactionClientBase(Client, ABC):
    """Abstract base class for HTTP clients that transparently handle translating
    requests and responses sent during a Fedora transaction as needed for the particular
    API version.

    Implemented by:

    * `Fedora4TransactionClient`
    * `Fedora6TransactionClient`
    """

    @classmethod
    def from_client(cls, client: Client, tx_uri: str, keep_alive: int = 90):
        """Build a `TransactionClient` from a regular `Client` object."""
        return cls(
            endpoint=client.endpoint,
            auth=client.session.auth,
            server_cert=client.session.verify,
            ua_string=client.ua_string,
            on_behalf_of=client.delegated_user,
            load_binaries=client.load_binaries,
            digest_algorithm=client.digest_algorithm,
            tx_uri=tx_uri,
            keep_alive=keep_alive,
        )

    def __init__(self, tx_uri: str, keep_alive: int = 90, **kwargs):
        super().__init__(**kwargs)
        self.tx: Transaction = Transaction(client=self, uri=tx_uri, keep_alive=keep_alive)
        """The `Transaction`"""

    @property
    def transaction_endpoint(self) -> str:
        """Immediately raises a `TransactionError`, since you cannot nest transactions."""
        raise TransactionError('Cannot nest transactions')

    @property
    def active(self) -> bool:
        """Whether a transaction is set and active."""
        return bool(self.tx and self.tx.active)

    @abstractmethod
    def request_maintain(self) -> Response:
        """Subclasses must implement this method with the API call needed to maintain the transaction."""
        raise NotImplementedError

    @abstractmethod
    def get_expiration(self, response: Response) -> str:
        """Subclasses must implement this method to extract the expiration timestamp from the response
        returned by the `request_maintain()` method."""
        raise NotImplementedError

    def maintain(self):
        """Maintains the active state of the transaction. Raises a `TransactionError` if the transaction
        is inactive, or there is a connection error or non-OK HTTP response from the repository server.

        Calls `request_maintain()` to do the actual HTTP request, and `get_expiration()` (with the
        `requests.Response` object) to get the expiration time."""
        logger.info(f'Maintaining transaction {self.tx}')
        if not self.active:
            raise TransactionError(f'Cannot maintain inactive transaction: {self.tx}')

        try:
            response = self.request_maintain()
        except ConnectionError as e:
            raise TransactionError(f'Failed to maintain transaction {self.tx}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Transaction {self.tx} is active until {self.get_expiration(response)}')
        else:
            raise TransactionError(
                f'Failed to maintain transaction {self.tx}: {response.status_code} {response.reason}'
            )

    @abstractmethod
    def request_commit(self) -> Response:
        """Subclasses must implement this method with the API call needed to commit the transaction."""
        raise NotImplementedError

    def commit(self):
        """Commits the transaction. Raises a `TransactionError` if the transaction is
        inactive, or there is a connection error or non-OK HTTP response from the repository
        server.

        Calls `request_commit()` to make the actual HTTP request."""
        logger.info(f'Committing transaction {self.tx}')
        if not self.active:
            raise TransactionError(f'Cannot commit inactive transaction: {self.tx}')

        self.tx.stop()
        try:
            response = self.request_commit()
        except ConnectionError as e:
            raise TransactionError(f'Failed to commit transaction {self.tx}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Committed transaction {self.tx}')
            return response
        else:
            raise TransactionError(f'Failed to commit transaction {self.tx}: {response.status_code} {response.reason}')

    @abstractmethod
    def request_rollback(self) -> Response:
        """Subclasses must implement this method with the API call needed to roll back the transaction."""
        raise NotImplementedError

    def rollback(self):
        """Rolls back the transaction. Raises a `TransactionError` if the transaction is
        inactive, or there is a connection error or non-OK HTTP response from the repository
        server.

        Calls `request_rollback()` to make the actual HTTP request."""
        logger.info(f'Rolling back transaction {self.tx}')
        if not self.tx.active:
            raise TransactionError(f'Cannot roll back inactive transaction: {self.tx}')

        self.tx.stop()
        try:
            response = self.request_rollback()
        except ConnectionError as e:
            raise TransactionError(f'Failed to roll back transaction {self.tx}: {e}') from e
        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f'Rolled back transaction {self.tx}')
            return response
        else:
            raise TransactionError(
                f'Failed to roll back transaction {self.tx}: {response.status_code} {response.reason}'
            )


class Fedora4TransactionClient(TransactionClientBase):
    """Transaction client that follows the Fedora 4 and 5 transactions API.

    See <https://wiki.lyrasis.org/spaces/FEDORA475/pages/90978100/RESTful+HTTP+API+-+Transactions>"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Send a POST request to this URL to keep the transaction alive
        self._maintenance_url = os.path.join(self.tx.uri, 'fcr:tx')
        # Send a POST request to this URL to commit the transaction
        self._commit_url = os.path.join(self.tx.uri, 'fcr:tx/fcr:commit')
        # Send a POST request to this URL to roll back the transaction
        self._rollback_url = os.path.join(self.tx.uri, 'fcr:tx/fcr:rollback')

    def request_maintain(self) -> Response:
        """`POST {transaction_url}/fcr:tx`"""
        return self.post(self._maintenance_url)

    def get_expiration(self, response: Response) -> str:
        """Returns the `Expires` header"""
        return response.headers['Expires']

    def request_commit(self) -> Response:
        """`POST {transaction_url}/fcr:tx/fcr:commit`"""
        return self.post(self._commit_url)

    def request_rollback(self) -> Response:
        """`POST {transaction_url}/fcr:tx/fcr:rollback`"""
        return self.post(self._rollback_url)

    def request(self, method: str, url: str, **kwargs) -> Response:
        """Makes sure the transaction keep-alive thread hasn't failed, and inserts the transaction
        id into the request URL. Then calls the `plastron.client.base.Client.request()` method with
        the same arguments.

        Raises a `RuntimeError` if the transaction keep-alive thread has failed."""
        if self.tx.keep_alive.failed.is_set():
            raise RuntimeError('Transaction keep-alive failed') from self.tx.keep_alive.exception

        request_url = str(self.insert_transaction_uri(URIRef(url)))
        return super().request(method, request_url, **kwargs)

    def get_location(self, response: Response) -> str | None:
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
        """Inserts the transaction id in to the request URL and calls
        `plastron.client.base.Client.get_description()` with the modified
        arguments. Then strips the transaction IDs out of the returned
        RDF and returns it to the caller."""
        text = super().get_description(
            url=str(self.insert_transaction_uri(URIRef(url))),
            accept=accept,
            include_server_managed=include_server_managed,
        )
        graph = self.remove_transaction_uri_for_graph(Graph().parse(data=text.value, format=text.media_type))
        value = graph.serialize(format=text.media_type) if graph is not None else ''
        return TypedText(text.media_type, value)

    def put_graph(self, url, graph: Graph | None) -> Response:
        """Insert the transaction id into the request URL and `graph`, then calls
        `plastron.client.base.Client.put_graph()` method with the modified arguments."""
        return super().put_graph(
            url=str(self.insert_transaction_uri(URIRef(url))),
            graph=self.insert_transaction_uri_for_graph(graph),
        )

    def patch_graph(self, url, deletes: Graph | None, inserts: Graph | None) -> Response:
        """Insert the transaction id into the request URL and the `inserts` and `deletes`
        graphs, then calls `plastron.client.base.Client.patch_graph()` method with the
        modified arguments."""
        return super().patch_graph(
            url=str(self.insert_transaction_uri(URIRef(url))),
            deletes=self.insert_transaction_uri_for_graph(deletes),
            inserts=self.insert_transaction_uri_for_graph(inserts),
        )

    def get_description_uri(self, uri: str, response: Response | None = None) -> str:
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

    def insert_transaction_uri_for_graph(self, graph: Graph | None) -> Graph | None:
        """Update URIs in subject and object positions of triples in the given `graph` to
        have the appropriate transaction ID, if needed."""
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

    def remove_transaction_uri_for_graph(self, graph: Graph | None) -> Graph | None:
        """Update URIs in subject and object positions of triples in the given `graph` to
        remove the appropriate transaction ID, if present."""
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


class Fedora6TransactionClient(TransactionClientBase):
    """Transaction client that follows the Fedora 6+ transactions API.

    See <https://wiki.lyrasis.org/spaces/FEDORA6x/pages/178882396/RESTful+HTTP+API+-+Transactions>"""

    def request_maintain(self) -> Response:
        """`POST {transaction_uri}`"""
        return self.post(self.tx.uri)

    def get_expiration(self, response: Response):
        """Returns the `Atomic-Expires` header"""
        return response.headers['Atomic-Expires']

    def request_commit(self) -> Response:
        """`PUT {transaction_uri}`"""
        return self.put(self.tx.uri)

    def request_rollback(self) -> Response:
        """`DELETE {transaction_uri}`"""
        return self.delete(self.tx.uri)

    def request(self, method: str, url: str, **kwargs) -> Response:
        """Makes sure the transaction keep-alive thread hasn't failed, and adds the transaction
        URI into the request headers as the `Atomic-ID`. Then calls `plastron.client.base.Client.request()`
        with the same arguments.

        Raises a `RuntimeError` if the transaction keep-alive thread has failed."""
        if self.tx.keep_alive.failed.is_set():
            raise RuntimeError('Transaction keep-alive failed') from self.tx.keep_alive.exception

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Atomic-ID'] = self.tx.uri
        return super().request(method, url, **kwargs)


class TransactionKeepAlive(threading.Thread):
    """Thread to run in the background while a long-running transaction is being
    processed, to ensure that the transaction does not time out due to inactivity.
    Based on <https://stackoverflow.com/a/12435256/5124907>"""

    def __init__(self, txn_client: TransactionClientBase, interval: int):
        """Create a transaction keep-alive thread."""
        super().__init__(name='TransactionKeepAlive')
        self.txn_client: TransactionClientBase = txn_client
        """The transaction client."""

        self.interval: int = interval
        """Time between transaction maintenance requests."""

        self.stopped: threading.Event = threading.Event()
        """Flag indicating whether this transaction has been stopped."""

        self.failed: threading.Event = threading.Event()
        """Flag indicating whether this transaction has failed."""

        self.exception: TransactionError | None = None
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
