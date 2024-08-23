import pytest

from plastron.jobs.importjob.spreadsheet import MetadataSpreadsheet
from plastron.models.umd import Item
from plastron.repo import Repository


@pytest.fixture
def repo():
    return Repository.from_url('http://localhost:8080/rest')


@pytest.fixture
def csv_filename(datadir):
    return datadir / 'embedded_objects.csv'


@pytest.fixture
def spreadsheet(csv_filename):
    return MetadataSpreadsheet(metadata_filename=csv_filename, model_class=Item)


@pytest.fixture
def rows(spreadsheet):
    return list(spreadsheet.rows())


def test_single_embed(repo, rows):
    item = rows[1].get_object(repo)

    assert len(item.creator) == 1


def test_multiple_embeds(repo, rows):
    item = rows[0].get_object(repo)

    assert len(item.creator) == 11


def test_single_embedded_contributor(repo, rows):
    item = rows[2].get_object(repo)

    assert len(item.contributor) == 1
