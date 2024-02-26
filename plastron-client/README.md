# plastron-client

HTTP client for connecting to an LDP server

## Quick Start

```python
from plastron.client import Client, Endpoint

endpoint = Endpoint('http://localhost:8080/fcrepo/rest')
client = Client(endpoint)

response = client.get('http://localhost:8080/fcrepo/rest/foobar123')

graph = client.get_graph('http://localhost:8080/fcrepo/rest/foobar123')
```