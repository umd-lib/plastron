from argparse import Namespace

import httpretty
import pytest
from rdflib import Graph, Literal

from plastron.cli.commands.find import find, Command
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
def test_find(datadir, repo, simulate_repo, matcher, properties, expected_count):
    graph = Graph().parse(file=(datadir / 'graph.ttl').open())
    simulate_repo(graph)
    resources = list(find(
        start_resource=repo['/container'],
        matcher=matcher,
        traverse=[ldp.contains],
        properties=properties,
    ))
    assert len(resources) == expected_count


@pytest.mark.parametrize(
    ('args', 'expected_paths'),
    [
        (
            Namespace(
                recursive='ldp:contains',
                data_properties=[],
                object_properties=[],
                types=[],
                match_all=True,
                match_any=False,
                uris=['/container'],
            ),
            ['/container', '/container/1', '/container/2'],
        ),
        (
            Namespace(
                recursive='ldp:contains',
                data_properties=[],
                object_properties=[],
                types=['pcdm:Object'],
                match_all=True,
                match_any=False,
                uris=['/container'],
            ),
            ['/container/1']
        ),
        (
            Namespace(
                recursive='ldp:contains',
                data_properties=[('dcterms:title', 'Moonpig')],
                object_properties=[],
                types=['pcdm:Object'],
                match_all=False,
                match_any=True,
                uris=['/container'],
            ),
            ['/container/1', '/container/2']
        ),
    ]
)
@httpretty.activate
def test_find_command(capsys, datadir, repo, simulate_repo, args, expected_paths):
    graph = Graph().parse(file=(datadir / 'graph.ttl').open())
    simulate_repo(graph)
    cmd = Command()
    cmd.repo = repo
    cmd(client=repo.client, args=args)
    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == len(expected_paths)
    for path in expected_paths:
        assert f'http://localhost:9999{path}\n' in captured.out
