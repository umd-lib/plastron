import csv
from unittest.mock import Mock

import pytest

from plastron.commands.importcommand import create_repo_changeset
from plastron.jobs import Row, build_fields
from plastron.models import Item


@pytest.fixture
def csv_data(datadir):
    csv_file = (datadir / 'embedded_objects.csv').open()
    yield csv.DictReader(csv_file)
    csv_file.close()


@pytest.fixture
def rows(csv_data):
    return [
        Row(line_reference=f'test:{n + 1}', row_number=n, data=row, identifier_column='Identifier')
        for n, row in enumerate(csv_data, start=1)
    ]


@pytest.fixture
def metadata(csv_data):
    metadata = Mock()
    metadata.fields = build_fields(csv_data.fieldnames, Item)
    metadata.model_class = Item
    return metadata


def test_single_embed(metadata, rows):
    changeset = create_repo_changeset(None, metadata, rows[1])

    assert len(changeset.item.creator) == 1


def test_multiple_embeds(metadata, rows):
    changeset = create_repo_changeset(None, metadata, rows[0])

    assert len(changeset.item.creator) == 11


def test_single_embedded_contributor(metadata, rows):
    changeset = create_repo_changeset(None, metadata, rows[2])

    assert len(changeset.item.contributor) == 1
