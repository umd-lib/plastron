from argparse import Namespace

import httpretty
from rdflib import Graph

from plastron.cli.commands.list import Command


@httpretty.activate
def test_list_command(capsys, datadir, repo, simulate_repo):
    graph = Graph().parse(file=(datadir / 'graph.ttl').open())
    simulate_repo(graph)
    cmd = Command()
    cmd.repo = repo
    cmd(client=repo.client, args=Namespace(long=False, uris=['/container']))
    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 2
    assert f'http://localhost:9999/container/1\n' in captured.out
    assert f'http://localhost:9999/container/2\n' in captured.out


@httpretty.activate
def test_list_long_command(capsys, datadir, repo, simulate_repo):
    graph = Graph().parse(file=(datadir / 'graph.ttl').open())
    simulate_repo(graph)
    cmd = Command()
    cmd.repo = repo
    cmd(client=repo.client, args=Namespace(long=True, uris=['/container']))
    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 2
    assert f'http://localhost:9999/container/1 Foobar\n' in captured.out
    assert f'http://localhost:9999/container/2 Moonpig\n' in captured.out
