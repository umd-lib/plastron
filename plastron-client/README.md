# plastron-client

HTTP client for connecting to an LDP server

## Quick Start

Here is the most basic usage of the `Endpoint` and `Client` classes:

```python
from plastron.client import Client, Endpoint

endpoint = Endpoint('http://localhost:8080/fcrepo/rest')
client = Client(endpoint)

response = client.get('http://localhost:8080/fcrepo/rest/foobar123')

graph = client.get_graph('http://localhost:8080/fcrepo/rest/foobar123')
```

## Basic Client

Beyond specifying the endpoint URL, you can also provide the following 
optional parameters to the `Client` class:

* **`auth`** Subclass of `requests.AuthBase` that provides authentication 
  and authorization for requests 
* **`server_cert`** Filename of the CA certificate to use to verify any 
  client certificates
* **`ua_string`** Custom User-Agent string. Default is `plastron/{version}`
* **`on_behalf_of`** Username of the user to *actually* run the request as;
  this requires that the initial connection to the repository be 
  established by a user with the `fedoraAdmin` role.
* **`load_binaries`** Whether to load the binary files when performing an 
  import
* **`session`** A `requests.Session` object to initialize the client with; 
  this can be used, for instance, to configure a caching session for the 
  client.

### HTTP Methods

For each HTTP method other than `OPTIONS`, there is a corresponding instance
method on a Client object, though spelled in lowercase:

| HTTP     | Plastron Client |
|----------|-----------------|
| `HEAD`   | `head()`        |
| `GET`    | `get()`         |
| `POST`   | `post()`        |
| `PUT`    | `put()`         |
| `PATCH`  | `patch()`       |
| `DELETE` | `delete()`      |

### Higher-Level Methods

Built on top of the basic HTTP methods are higher-level methods for creating
and modifying resources in the repository:

#### Getting Resources

* `exists()`
* `get_description_uri()`
* `get_description()`
* `get_graph()`

#### Creating Resources

* `create()`
* `create_at_path()`
* `create_in_container()`

#### Updating Resources

* `put_graph()`
* `patch_graph()`

## Proxied Client

For situations where you need to use a connection URL that is different from
the canonical base URL used by the repository, there is the proxied client.
The use case that led to the creation of this class was the desire to use
Plastron to connect to our Fedora repository from other services that were
running within the same Kubernetes cluster as the repository. In this case, we
wanted to avoid using the public DNS name for the repository and save
ourselves the network traffic and cost overhead of leaving the cluster only to
come right back in. However, we still needed Fedora to serialize its RDF using
the public DNS base URL.

The proxied client accomplishes this by adding an `origin_endpoint` parameter
that provides the base URL that should be used for actual network connections.
The `endpoint` parameter is used to set `X-Forwarded-Host` and
`X-Forwarded-Proto` headers in all outbound requests. Fedora uses these
headers (if present) to construct its canonical base URL for RDF
serialization.

```python
from plastron.client import Client, Endpoint
from plastron.client.proxied import ProxiedClient

client = ProxiedClient(
    endpoint=Endpoint('https://fcrepo.lib.umd.edu/fcrepo/rest'),
    origin_endpoint=Endpoint('http://fcrepo-webapp:8080/fcrepo/rest'),
)
```

## Transactions

This library supports transactions, both as implemented in Fedora 4-5 and as
implemented in Fedora 6+. The system will automatically determine which
transaction API is in use by examining the `Link` headers returned from the
request to create a transaction. The presence of a `Link` header with the
`rel` attribute `http://fedora.info/definitions/v4/transaction#commitEndpoint`
indicates that this is a Fedora 6+ style of transaction; otherwise, we assume
a Fedora 4-5 transaction style.

```python
from plastron.client import Client, Endpoint
from plastron.client.transactions import transaction

# create an Endpoint and Client as before
endpoint = Endpoint('http://localhost:8080/fcrepo/rest')
client = Client(endpoint)

# use the transaction() context manager to open a transaction
# tx_client is an instance of a transaction-aware subclass of Client,
# either Fedora4TransactionClient of Fedora6TransactionClient.
with transaction(client) as tx_client:
    # use all the normal request methods to interact with the repository
    response = tx_client.post('...')
```

You do not need to explicitly call `tx_client.commit()` if you are using the
context manager; it will automatically call it for you at the end of the
block. Similarly, if any code inside the block raises an exception, the
context manager will try to roll back the transaction using
`tx_client.rollback()`.

Since there is an expiration time for transactions in the Fedora API, when we
open a transaction in Plastron we also spawn a "keep-alive" thread to
continually ping the transaction URL. This will continue to extend the
expiration time for as long as we are working in the transaction. The default
ping interval is 90 seconds (Fedora's default transaction timeout is 3
minutes). You can set a different length of time when calling `transaction()`.

```python
from plastron.client import Client, Endpoint
from plastron.client.transactions import transaction

# create an Endpoint and Client as before
endpoint = Endpoint('http://localhost:8080/fcrepo/rest')
client = Client(endpoint)

# use 60 second keep-alive pings instead of the default
with transaction(client, keep_alive=60) as tx_client:
    ...
```
