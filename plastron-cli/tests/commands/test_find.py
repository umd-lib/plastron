import pytest
from rdflib import Graph, Literal

from plastron.client import ResourceURI
from plastron.cli.commands.find import Command
from plastron.namespaces import rdf, pcdm, dcterms


@pytest.fixture
def resources(datadir):
    source_graph = Graph()
    source_graph.parse(datadir / 'graph.ttl', format='turtle')

    resource_graphs = []
    for subject in set(source_graph.subjects()):
        graph = Graph()
        for triple in source_graph.triples((subject, None, None)):
            graph.add(triple)
        resource_graphs.append((ResourceURI(subject, subject), graph))
    return resource_graphs


@pytest.mark.parametrize(
    ['match_condition', 'property_filter', 'expected_count'],
    [
        (all, [], 2),
        (all, [(rdf.type, pcdm.Object)], 1),
        (all, [(rdf.type, pcdm.Object), (dcterms.title, Literal('Moonpig'))], 0),
        (any, [(rdf.type, pcdm.Object), (dcterms.title, Literal('Moonpig'))], 2),
    ]
)
def test_find(resources, match_condition, property_filter, expected_count):
    cmd = Command()
    cmd.resource_count = 0
    cmd.properties = property_filter
    cmd.match = match_condition
    for resource, graph in resources:
        cmd.find(resource, graph)
    assert cmd.resource_count == expected_count
