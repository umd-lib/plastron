import logging
from unittest.mock import MagicMock

import pytest
from rdflib import URIRef, Literal

from plastron.cli.commands.set import get_new_values, set_fields
from plastron.models import Item
from plastron.rdfmapping.resources import RDFResource
from plastron.repo import RepositoryResource, Repository


@pytest.mark.parametrize(
    ('model_class', 'fields_to_set', 'expected_values'),
    [
        (RDFResource, [], {}),
        (
            RDFResource,
            [
                ('rdf_type', 'foaf:Agent'),
                ('label', 'foobar'),
            ],
            {
                'rdf_type': {URIRef('http://xmlns.com/foaf/0.1/Agent')},
                'label': {Literal('foobar')},
            }
        ),
        (
            RDFResource,
            [
                ('label', 'foo'),
                ('rdf_type', 'foaf:Agent'),
                ('label', 'bar'),
            ],
            {
                'rdf_type': {URIRef('http://xmlns.com/foaf/0.1/Agent')},
                'label': {Literal('foo'), Literal('bar')},
            }
        ),
    ]
)
def test_get_new_values(model_class, fields_to_set, expected_values):
    assert get_new_values(model_class, fields_to_set) == expected_values


@pytest.mark.parametrize(
    ('fields_to_set', 'expected_log_message', 'should_update'),
    [
        (
            # valid update
            [
                ('title', 'foobar'),
                ('identifier', '123'),
                ('object_type', 'http://purl.org/dc/dcmitype/Text'),
                ('rights', 'http://vocab.lib.umd.edu/rightsStatement#InC'),
            ],
            'Resource /foo is a valid Item',
            True,
        ),
        (
            # invalid update (missing required fields)
            [
                ('title', 'foobar'),
            ],
            'Resource /foo is invalid, skipping',
            False,
        ),
        (
            # invalid update (wrong vocabulary)
            [
                ('title', 'foobar'),
                ('identifier', '123'),
                ('object_type', 'http://purl.org/dc/dcmitype/Text'),
                ('rights', 'http://vocab.lib.umd.edu/form#letter'),
            ],
            'Resource /foo is invalid, skipping',
            False,
        ),
    ],

)
def test_set_fields(monkeypatch, caplog, fields_to_set, expected_log_message, should_update):
    caplog.set_level(logging.INFO)
    mock_repo = MagicMock(spec=Repository)
    mock_item = Item()
    mock_resource = RepositoryResource(repo=mock_repo)
    monkeypatch.setattr(mock_resource, 'read', lambda: mock_resource)
    monkeypatch.setattr(mock_resource, 'describe', lambda _: mock_item)
    mock_update = MagicMock()
    monkeypatch.setattr(mock_resource, 'update', mock_update)
    mock_repo.__getitem__.return_value = mock_resource
    mock_context = MagicMock(obj=MagicMock(repo=mock_repo))
    set_fields(
        mock_context,
        model_name='Item',
        fields_to_set=fields_to_set,
        uris=['/foo'],
    )
    assert expected_log_message in caplog.text
    assert mock_update.call_count == (1 if should_update else 0)
