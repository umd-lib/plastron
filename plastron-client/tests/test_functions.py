import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from rdflib import Graph, URIRef, Literal

from plastron.client import Client, paths_to_create, build_sparql_update


@pytest.fixture
def empty_graph():
    return Graph()


@pytest.fixture
def non_empty_graph():
    graph = Graph()
    graph.add((URIRef('http://example.com/subject'), URIRef('http://purl.org/dc/terms/title'), Literal('Moonpig')))
    return graph


def test_paths_to_create():
    mock_client = MagicMock(spec=Client)
    mock_client.path_exists.return_value = True

    assert paths_to_create(mock_client, Path('/foo')) == []


def test_build_sparql_update(empty_graph, non_empty_graph):
    assert build_sparql_update(delete_graph=empty_graph, insert_graph=empty_graph) == ''
    assert re.match(
        r'^DELETE DATA {.*}$',
        build_sparql_update(delete_graph=non_empty_graph, insert_graph=empty_graph),
    )
    assert re.match(
        r'^INSERT DATA {.*}',
        build_sparql_update(delete_graph=empty_graph, insert_graph=non_empty_graph),
    )
    assert re.match(
        r'^DELETE {.*} INSERT {.*} WHERE {}$',
        build_sparql_update(delete_graph=non_empty_graph, insert_graph=non_empty_graph),
    )
