from typing import Optional

from rdflib import Graph, URIRef
from rdflib.term import Node


def update_node(node: Node, old_uri: URIRef, new_uri: URIRef) -> Optional[URIRef]:
    """If ``node`` equals the ``old_uri`` (with an optional fragment identifier),
    replace ``old_uri`` with ``new_uri`` and return a new URIRef object. Otherwise,
    return ``None``."""
    if node == old_uri or str(node).startswith(old_uri + '#'):
        return URIRef(str(node).replace(old_uri, new_uri))
    else:
        return None


class TrackChangesGraph(Graph):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.inserts = Graph()
        self.deletes = Graph()

    def change_uri(self, old_uri: URIRef, new_uri: URIRef):
        """Change occurrences of ``old_uri`` to ``new_uri`` in this graph.
        This includes URIRefs that contain a fragment identifier following
        the ``old_uri``.

        This object is updated in place."""
        for s, p, o in self:
            new_s = update_node(s, old_uri, new_uri)
            new_p = update_node(p, old_uri, new_uri)
            new_o = update_node(o, old_uri, new_uri)
            if new_s or new_p or new_o:
                super().remove((s, p, o))
                super().add((new_s or s, new_p or p, new_o or o))

    def add(self, triple):
        try:
            self.deletes.remove(triple)
        except KeyError:
            # this is the case where this triple has not been deleted
            pass
        self.inserts.add(triple)

    def remove(self, triple):
        try:
            self.inserts.remove(triple)
        except KeyError:
            # this is the case where this triple has not been inserted
            pass
        self.deletes.add(triple)

    @property
    def has_changes(self) -> bool:
        return len(self.inserts) > 0 or len(self.deletes) > 0

    def with_changes(self) -> Graph:
        graph = Graph()
        for triple in self:
            graph.add(triple)
        for triple in self.deletes:
            graph.remove(triple)
        for triple in self.inserts:
            graph.add(triple)
        return graph

    def apply_changes(self):
        for triple in self.deletes:
            super().remove(triple)
        for triple in self.inserts:
            super().add(triple)
        self.clear_changes()

    def clear_changes(self):
        self.inserts = Graph()
        self.deletes = Graph()
