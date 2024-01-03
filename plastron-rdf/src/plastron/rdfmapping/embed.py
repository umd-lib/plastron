from typing import TypeVar, Callable, Type, Mapping, Any
from uuid import uuid4

from rdflib import URIRef
from urlobject import URLObject

T = TypeVar('T')


class EmbeddedObject:
    """Wrapper object to delay instantiation of inline-specified objects
    that should be embedded in their parent instance (i.e., share a graph
    object)."""
    def __init__(self, cls: Type[T], fragment_id: str = None, **kwargs):
        self.cls: Type[T] = cls
        """Model class to use for the embedded object"""

        self.fragment_id: str = fragment_id or str(uuid4())
        """Fragment identifier for the embedded object. Defaults to a new UUID."""

        self.kwargs: Mapping[str, Any] = kwargs
        """Keyword arguments to use when constructing the embedded object."""

    def embed(self, instance) -> T:
        """Instantiate an object with the stored class and keyword arguments,
        with a URI from the parent instance object plus the stored fragment ID.
        The `instance` object must have `uri` and `graph` attributes."""
        return self.cls(
            uri=URIRef(URLObject(instance.uri).with_fragment(self.fragment_id)),
            graph=instance.graph,
            **self.kwargs,
        )


def embedded(cls: Type[T]) -> Callable[..., EmbeddedObject]:
    """Function to support an alternative syntax of calling the EmbeddedObject
    constructor. Instead of:

    ```python
    r = MyModel(
        prop=EmbeddedObject(
            MyOtherModel,
            foo=Literal('bar'),
        ),
    )
    ```

    You can use:

    ```python
    r = MyModel(
        prop=embedded(MyOtherModel)(
            foo=Literal('bar'),
        ),
    )
    ```
    """
    def _embedded(**kwargs):
        return EmbeddedObject(cls, **kwargs)
    return _embedded
