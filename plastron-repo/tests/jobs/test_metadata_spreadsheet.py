import pytest

from plastron.jobs.importjob import MetadataSpreadsheet
from plastron.models import Item


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



