from argparse import ArgumentParser

import pytest
from rdflib import Literal, URIRef

from plastron.cli import load_commands, parse_data_property, parse_object_property
from plastron.cli.commands import importcommand
from plastron.namespaces import dcterms, rdf, pcdm


def test_find_commands_correctly_handles_import_command():
    # The "import" command is special, because the name of the command
    # is "import", but the module is "importcommand"
    parser = ArgumentParser(prog='plastron', description='Batch operation tool for Fedora 4.')
    parser.set_defaults(cmd_name=None)

    subparsers = parser.add_subparsers(title='commands')

    command_modules = load_commands(subparsers)
    assert 'import' in command_modules
    assert command_modules['import'] == importcommand


@pytest.mark.parametrize(
    ('p', 'o', 'expected'),
    [
        ('dcterms:title', 'Foobar', (dcterms.title, Literal('Foobar'))),
        ('dcterms:title', '"der Hund"@de', (dcterms.title, Literal('der Hund', lang='de'))),
    ],
)
def test_parse_data_property(p, o, expected):
    assert parse_data_property(p, o) == expected


@pytest.mark.parametrize(
    ('p', 'o', 'expected'),
    [
        ('rdf:type', 'pcdm:Object', (rdf.type, pcdm.Object)),
        ('dcterms:creator', 'https://www.lib.umd.edu/', (dcterms.creator, URIRef('https://www.lib.umd.edu/'))),
        ('dcterms:creator', '<https://www.lib.umd.edu/>', (dcterms.creator, URIRef('https://www.lib.umd.edu/'))),
    ],
)
def test_parse_object_property(p, o, expected):
    assert parse_object_property(p, o) == expected
