from argparse import Namespace

import httpretty
from rdflib import Graph

from plastron.cli.commands.list import Command


@httpretty.activate
def test_list_command(capsys, datadir, plastron_context, simulate_repo):
    graph = Graph().parse(file=(datadir / 'graph.ttl').open())
    simulate_repo(graph)
    args = Namespace(long=False, uris=['/container'], delegated_user=None)
    plastron_context.args = args
    cmd = Command(context=plastron_context)
    cmd(args)
    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 2
    assert 'http://localhost:9999/container/1\n' in captured.out
    assert 'http://localhost:9999/container/2\n' in captured.out


@httpretty.activate
def test_list_long_command(capsys, datadir, plastron_context, simulate_repo):
    graph = Graph().parse(file=(datadir / 'graph.ttl').open())
    simulate_repo(graph)
    args = Namespace(long=True, uris=['/container'], delegated_user=None)
    plastron_context.args = args
    cmd = Command(context=plastron_context)
    cmd(args)
    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 2
    assert 'http://localhost:9999/container/1 Foobar\n' in captured.out
    assert 'http://localhost:9999/container/2 Moonpig\n' in captured.out
