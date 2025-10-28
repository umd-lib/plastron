# plastron-repo

Fedora repository resources and operations

## Repository

There are several class methods on the `Repository` class available as 
shortcuts to quickly create a Repository object.

```python
# simple repo, no authentication
from plastron.repo import Repository

repo = Repository.from_url('http://localhost:8080/fcrepo/rest')
```

```python
# repo with HTTP Basic authentication
from plastron.repo import Repository
from requests.auth import HTTPBasicAuth

repo = Repository.from_url(
    url='http://localhost:8080/fcrepo/rest',
    # any class that implements requests.auth.AuthBase will work here
    auth=HTTPBasicAuth('username', 'password'),
)
```

```python
# repo with settings from a configuration dictionary
from plastron.repo import Repository

repo = Repository.from_config({
    'REST_ENDPOINT': 'http://localhost:8080/fcrepo/rest',
    'RELPATH': '/',
    'REPO_EXTERNAL_URL': 'http://fcrepo-local/fcrepo/rest',
    'SERVER_CERT': 'path/to/cert.pem',
    # authentication section
    'FEDORA_USER': 'username',
    'FEDORA_PASSWORD': 'password',
})
```

```python
# repo with settings from a configuration YAML file;
# the structure of the YAML file is the same as the
# configuration dictionary
from plastron.repo import Repository

repo = Repository.from_config_file('config.yml')
```

You can always use the full constructor as well.

```python
from requests.auth import HTTPBasicAuth

from plastron.client import Client, Endpoint
from plastron.repo import Repository

repo = Repository(
    client=Client(
        endpoint=Endpoint(
            url='http://localhost:8080/fcrepo/rest',
            default_path='/',
            external_url='http://fcrepo-local/fcrepo/rest',
        ),
        auth=HTTPBasicAuth('username', 'password'),
        server_cert='path/to/cert.pem',
        ua_string='my-client/3.14',
        on_behalf_of='user',
    )
)
```

## Repository Resources

The *repository resource classes* represent a single URL-addressable HTTP 
resource that is stored in a repository. The most basic of these is 
`RepositoryResource`.

```python
from plastron.repo import Repository, RepositoryResource

repo = Repository.from_url('http://localhost:8080/fcrepo/rest')

# Repository object support indexing with a path
resource = repo['/foo']
assert resource.url == 'http://localhost:8080/fcrepo/rest/foo'
assert isinstance(resource, RepositoryResource)

# this is merely syntactic sugar for the get_resource() method
resource = repo.get_resource('/foo')
assert resource.url == 'http://localhost:8080/fcrepo/rest/foo'
assert isinstance(resource, RepositoryResource)
```

When getting a resource from a Repository, you can provide a class to
instantiate (it defaults to `RepositoryResource`). So to get an LDP container
representation of a resource, use `ContainerResource` instead.

```python
from plastron.repo import Repository, ContainerResource

repo = Repository.from_url('http://localhost:8080/fcrepo/rest')

container = repo.get_resource('/bar', ContainerResource)
assert container.url == 'http://localhost:8080/fcrepo/rest/bar'
assert isinstance(container, ContainerResource)

# you can also use slice notation to provide the class
container = repo['/bar':ContainerResource]
assert container.url == 'http://localhost:8080/fcrepo/rest/bar'
assert isinstance(container, ContainerResource)
```

## Retrieving Data

None of the methods above actually send any HTTP requests. To connect to 
the repository and retrieve data, use the `read()` method on a resource.

## RDF Descriptions

Repository resources can be *described* using RDF resource classes defined
using the [plastron-rdf](../plastron-rdf) library. A *resource 
description* is an RDF resource class that shares its underlying RDFLib 
Graph object with a repository resource. Thus changes to the resource 
description object will be reflected in the repository resource's graph 
and will be saved when the repository resource is written back to the 
repository.

Here is an (extremely simplified) example of retrieving a resource, 
updating some properties, and writing it back to the repository.

```python
from rdflib import Literal, URIRef

from plastron.rdfmapping.resources import RDFResource
from plastron.repo import Repository, ContainerResource

repo = Repository.from_url('http://localhost:8080/fcrepo/rest')
resource = repo['/obj/123':ContainerResource]
resource.read()

# RDFResource is a very simple RDF description class
# it maps rdf:type to rdf_type and rdfs:label to label
obj = resource.describe(RDFResource)
obj.label = Literal('Digital Object 123')
obj.rdf_type.add(URIRef('http://pcdm.org/models#Object'))

assert obj.has_changes

# sends a PATCH request to the repository with a SPARQL
# update query built from the differences between the
# graph read from the repo and the current graph
resource.update()
```

## Binaries
