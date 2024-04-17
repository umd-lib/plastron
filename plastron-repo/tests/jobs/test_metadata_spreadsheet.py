from unittest.mock import MagicMock

import pytest
from rdflib import Literal

from plastron.jobs.importjob import MetadataSpreadsheet
from plastron.models import Item
from plastron.namespaces import umdtype
from plastron.repo import Repository


def test_no_binaries(datadir):
    metadata = MetadataSpreadsheet(datadir / 'no_binaries.csv', Item)
    assert not metadata.has_binaries


@pytest.mark.parametrize(
    ('filename',),
    [
        ('has_files.csv',),
        ('has_item_files.csv',),
        ('has_files_and_item_files.csv',),
    ]
)
def test_has_binaries(filename, datadir):
    metadata = MetadataSpreadsheet(datadir / filename, Item)
    assert metadata.has_binaries


def test_column_with_datatype(datadir):
    metadata = MetadataSpreadsheet(datadir / 'postcard.csv', Item)
    repo = MagicMock(spec=Repository)
    row = next(metadata.rows())
    item = row.get_object(repo)
    assert item.handle.value == Literal('hdl:1903.1/5982', datatype=umdtype.handle)


def test_cannot_mix_language_and_datatype(datadir):
    metadata = MetadataSpreadsheet(datadir / 'bad_postcard.csv', Item)
    repo = MagicMock(spec=Repository)
    row = next(metadata.rows())
    with pytest.raises(RuntimeError):
        row.get_object(repo)
