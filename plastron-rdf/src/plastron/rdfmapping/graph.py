import pathlib
from typing import Optional, Union, IO, TextIO, BinaryIO, Any

from rdflib import Graph, URIRef
from rdflib.parser import InputSource
from rdflib.term import Node


def copy_triples(src: Graph, dest: Graph):
    for triple in src:
        dest.add(triple)


def update_node(node: Node, old_uri: URIRef, new_uri: URIRef) -> Node:
    """If ``node`` equals the ``old_uri`` (with an optional fragment identifier),
    replace ``old_uri`` with ``new_uri`` and return a new URIRef object. Otherwise,
    return ``None``."""
    if node == old_uri or str(node).startswith(old_uri + '#'):
        return URIRef(str(node).replace(old_uri, new_uri))
    else:
        return node


def new_triple(old_uri, new_uri, s, p, o):
    new_s = update_node(s, old_uri, new_uri)
    new_p = update_node(p, old_uri, new_uri)
    new_o = update_node(o, old_uri, new_uri)
    return new_s, new_p, new_o


class TrackChangesGraph(Graph):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.original = Graph()

    def parse(
        self,
        source: Optional[
            Union[IO[bytes], TextIO, InputSource, str, bytes, pathlib.PurePath]
        ] = None,
        publicID: Optional[str] = None,  # noqa: N803
        format: Optional[str] = None,
        location: Optional[str] = None,
        file: Optional[Union[BinaryIO, TextIO]] = None,
        data: Optional[Union[str, bytes]] = None,
        **args: Any,
    ) -> 'TrackChangesGraph':
        super().parse(source, publicID, format, location, file, data, **args)
        self.original = Graph()
        copy_triples(self, self.original)
        return self

    def change_uri(self, old_uri: URIRef, new_uri: URIRef):
        """Change occurrences of ``old_uri`` to ``new_uri`` in this graph.
        This includes URIRefs that contain a fragment identifier following
        the ``old_uri``.

        This object is updated in place."""
        for s, p, o in self:
            new_s, new_p, new_o = new_triple(old_uri, new_uri, s, p, o)
            self.remove((s, p, o))
            self.add((new_s, new_p, new_o))

    @property
    def inserts(self) -> Graph:
        return self - self.original

    @property
    def deletes(self) -> Graph:
        return self.original - self

    @property
    def has_changes(self) -> bool:
        return len(self.inserts) > 0 or len(self.deletes) > 0

    def apply_changes(self):
        graph = Graph()
        copy_triples(self, graph)
        self.original = graph
