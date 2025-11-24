from rdflib.plugins.sparql import prepareUpdate

from plastron.rdfmapping.graph import TrackChangesGraph


# https://umd-dit.atlassian.net/browse/LIBFCREPO-1739
# rdflib 7.3+ does not currently work with the TrackChangesGraph
# due to an explicit type check in rdflib.plugins.sparql.update.
# A comment above this type check code indicates that the rdflib
# maintainers intend to remove this type check once a series of
# modifications and deprecations in the ConjunctiveGraph and
# Dataset classes. For the time-being, though, we are pinning
# rdflib to a version before this type check was introduced.
#
# This test is in place to ensure that the update functionality
# of the installed version of rdflib works with TrackChangesGraph
def test_rdflib_graph_type_check():
    graph = TrackChangesGraph()
    graph.update(prepareUpdate('DELETE {} INSERT {<> <http://purl.org/dc/terms/title> "Moonpig"} WHERE {}'))
    assert len(graph) == 1
