from typing import TypeVar, Optional, Callable
from uuid import uuid4

from rdflib import URIRef
from urlobject import URLObject

T = TypeVar('T')


class EmbeddedObject:
    """Wrapper object to delay instantiation of inline-specified objects
    that should be embedded in their parent instance (i.e., share a graph
    object)."""
    def __init__(self, cls: T, fragment_id: Optional[str] = None, **kwargs):
        self.cls = cls
        self.fragment_id = fragment_id or str(uuid4())
        self.kwargs = kwargs

    def embed(self, instance):
        """Instantiate an object with the stored class and keyword arguments,
        with a URI from the instance object plus the stored fragment ID."""
        return self.cls(
            uri=URIRef(URLObject(instance.uri).with_fragment(self.fragment_id)),
            graph=instance.graph,
            **self.kwargs,
        )


def embedded(cls: T) -> Callable[..., EmbeddedObject]:
    """Function to support an alternative syntax of calling the EmbeddedObject
    constructor. Instead of::

        r = MyModel(
            prop=EmbeddedObject(
                MyOtherModel,
                foo=Literal('bar'),
            ),
        )

    You can use::

        r = MyModel(
            prop=embedded(MyOtherModel)(
                foo=Literal('bar'),
            ),
        )
    """
    def _embedded(**kwargs):
        return EmbeddedObject(cls, **kwargs)
    return _embedded
