import httpretty
import pytest
from rdflib import Graph, Literal

from plastron.cli.commands.find import find
from plastron.namespaces import rdf, pcdm, dcterms, ldp


@pytest.mark.parametrize(
    ['matcher', 'properties', 'expected_count'],
    [
        (all, [], 3),
        (all, [(rdf.type, pcdm.Object)], 1),
        (all, [(rdf.type, pcdm.Object), (dcterms.title, Literal('Moonpig'))], 0),
        (any, [(rdf.type, pcdm.Object), (dcterms.title, Literal('Moonpig'))], 2),
    ]
)
@httpretty.activate
def test_find(datadir, repo, register_root, simulate_repo, matcher, properties, expected_count):
    register_root()
    graph = Graph().parse(file=(datadir / 'graph.ttl').open())
    simulate_repo(graph)
    resources = list(find(
        start_resource=repo['/container'],
        matcher=matcher,
        traverse=[ldp.contains],
        properties=properties,
    ))
    assert len(resources) == expected_count
