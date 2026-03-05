import logging
import os
from base64 import urlsafe_b64encode
from collections import namedtuple
from typing import NamedTuple

from rdflib import Graph, Literal, URIRef

logger = logging.getLogger(__name__)

OMIT_SERVER_MANAGED_TRIPLES = 'return=representation; omit="http://fedora.info/definitions/v4/repository#ServerManaged"'


def random_slug(length: int = 6) -> str:
    """Generate a URL-safe random string of characters. Uses `os.urandom()`
    as its source of entropy."""
    return urlsafe_b64encode(os.urandom(length)).decode()


def serialize(graph: Graph, **kwargs):
    p: URIRef
    o: URIRef | Literal

    logger.info('Including properties:')
    for _, p, o in graph:
        pred = p.n3(namespace_manager=graph.namespace_manager)
        obj = o.n3(namespace_manager=graph.namespace_manager)
        logger.info(f'  {pred} {obj}')
    return graph.serialize(**kwargs)


class ResourceURI(namedtuple('Resource', ['uri', 'description_uri'])):
    """Lightweight representation of a resource URI and URI of its description
    For RDFSources, in general the uri and description_uri will be the same."""

    __slots__ = ()

    def __str__(self):
        return self.uri


class TypedText(NamedTuple):
    """Data object combining a string value and its media type,
    expressed as a MIME type string.

    ```pycon
    >>> json_data = TypedText('application/json', '{"foo": 1, "bar": "two"}')
    >>> json_data
    TypedText(media_type='application/json', value='{"foo": 1, "bar": "two"}')
    ```

    Supports `str()`, `len()`, and `bool()`. Returns the string value, the
    length of the string value, and the boolean cast of the string value,
    respectively:

    ```pycon
    >>> str(json_data)
    '{"foo": 1, "bar": "two"}'

    >>> len(json_data)
    24

    >>> bool(json_data)
    True
    ```

    Two `TypedText` objects are only equal if both the string value
    and the media type match:

    ```pycon
    >>> json_data = TypedText('application/json', '{"foo": 1, "bar": "two"}')
    >>> text_data = TypedText('text/plain', '{"foo": 1, "bar": "two"}')

    >>> json_data.value == text_data.value
    True

    >>> json_data == text_data
    False
    ```
    """

    media_type: str
    """MIME type, e.g. "text/plain" or "application/ld+json" """

    value: str
    """string value"""

    def __str__(self):
        return self.value

    def __bool__(self):
        return bool(self.value)

    def __len__(self):
        return len(self.value)


class SessionHeaderAttribute:
    """Descriptor that maps an attribute to a session header name. Requires
    the instance to have a `session` attribute with a `headers` attribute whose
    value is a mapping that supports the methods `get()` and `update()`, plus
    the `del` operator. For example:

    ```python
    from requests import Session

    class Foo:
        ua_string = SessionHeaderAttribute('User-Agent')

        def __init__(self):
            self.session = Session()

    # initially not set
    foo = Foo()
    assert 'User-Agent' not in foo.session.headers

    # set the header
    foo.ua_string = 'MyClient/1.0.0'
    assert foo.session.headers['User-Agent'] == 'MyClient/1.0.0'

    # change the header
    foo.ua_string = 'OtherAgent/2.0.0'
    assert foo.session.headers['User-Agent'] == 'OtherAgent/2.0.0'

    # remove the header
    del foo.ua_string
    assert 'User-Agent' not in foo.session.headers
    ```
    """

    def __init__(self, header_name: str):
        self.header_name = header_name
        """The HTTP header name"""

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.session.headers.get(self.header_name, None)

    def __set__(self, instance, value):
        if value is not None:
            instance.session.headers.update({self.header_name: str(value)})

    def __delete__(self, instance):
        try:
            del instance.session.headers[self.header_name]
        except KeyError:
            pass


def build_sparql_update(delete_graph: Graph = None, insert_graph: Graph = None) -> str:
    """Build a SPARQL Update Query given the two graphs:

    * If there are no deletes (i.e., `delete_graph` contains no triples, or
      is set to `None`), returns an `INSERT DATA { ... }` statement;
    * If there are no inserts (i.e., `insert_graph` contains no triples, or
      is set to `None`), returns a `DELETE DATA { ... }` statement;
    * If there are both deletes and inserts, returns a full `DELETE { ... } INSERT { ... }
      WHERE {}` statement (the `WHERE` clause is always empty);
    * If there are neither inserts nor deletes, returns the empty string."""
    if delete_graph is not None and len(delete_graph) > 0:
        deletes = delete_graph.serialize(format='nt').strip()
    else:
        deletes = None

    if insert_graph is not None and len(insert_graph) > 0:
        inserts = insert_graph.serialize(format='nt').strip()
    else:
        inserts = None

    if deletes is not None and inserts is not None:
        return f"DELETE {{ {deletes} }} INSERT {{ {inserts} }} WHERE {{}}"
    elif deletes is not None:
        return f"DELETE DATA {{ {deletes} }}"
    elif inserts is not None:
        return f"INSERT DATA {{ {inserts} }}"
    else:
        return ''
