import pathlib
from typing import Optional, IO, TextIO, BinaryIO, Any

from rdflib import Graph, URIRef
from rdflib.parser import InputSource
from rdflib.term import Node


def copy_triples(src: Graph, dest: Graph):
    """Add all triples in `src` to `dest`."""
    for triple in src:
        dest.add(triple)


def update_node(node: Node, old_uri: URIRef, new_uri: URIRef) -> Node:
    """If `node` equals the `old_uri` (with an optional fragment identifier),
    replace `old_uri` with `new_uri` and return a new URIRef object. Otherwise,
    return the original `node`."""
    if node == old_uri or str(node).startswith(old_uri + '#'):
        return URIRef(str(node).replace(old_uri, new_uri))
    else:
        return node


def new_triple(old_uri: URIRef, new_uri: URIRef, s: Node, p: Node, o: Node) -> tuple[Node, Node, Node]:
    """Update instances of `old_uri` to `new_uri` in `s`, `p`, and `o`,
    and return the updated triple."""
    new_s = update_node(s, old_uri, new_uri)
    new_p = update_node(p, old_uri, new_uri)
    new_o = update_node(o, old_uri, new_uri)
    return new_s, new_p, new_o


class TrackChangesGraph(Graph):
    """An RDF graph that tracks inserts and deletes."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.original = Graph()
        """Original graph"""

    def parse(
        self,
        source: Optional[
            IO[bytes] | TextIO | InputSource | str | bytes | pathlib.PurePath
        ] = None,
        publicID: Optional[str] = None,  # noqa: N803
        format: Optional[str] = None,
        location: Optional[str] = None,
        file: Optional[BinaryIO | TextIO] = None,
        data: Optional[str | bytes] = None,
        **args: Any,
    ) -> 'TrackChangesGraph':
        """Parses the graph normally, and then saves a copy of the original."""
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
        """Graph containing triples that have been added"""
        return self - self.original

    @property
    def deletes(self) -> Graph:
        """Graph containing triples that have been removed"""
        return self.original - self

    @property
    def has_changes(self) -> bool:
        """Whether this graph has been changed"""
        return len(self.inserts) > 0 or len(self.deletes) > 0

    def apply_changes(self):
        """Replace the original graph with the current graph. Immediately
        after calling this method, `has_changes()` will return `False`."""
        graph = Graph()
        copy_triples(self, graph)
        self.original = graph
