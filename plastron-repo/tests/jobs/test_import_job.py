from contextlib import nullcontext
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from plastron.cli.commands.publish import get_publication_status
from plastron.jobs.imports import ImportJobs, ImportConfig
from plastron.repo import Repository, RepositoryResource


@pytest.fixture
def jobs_dir(datadir) -> Path:
    return datadir


@pytest.fixture
def jobs(jobs_dir) -> ImportJobs:
    return ImportJobs(jobs_dir)


@pytest.mark.parametrize(
    ('job_id', 'safe_id'),
    [
        # basic
        ('foo', 'foo'),
        # with slashes
        ('foo/bar', 'foo%2Fbar'),
        # URI as job ID
        ('http://localhost:3000/import-jobs/17', 'http%3A%2F%2Flocalhost%3A3000%2Fimport-jobs%2F17'),
    ]
)
def test_safe_job_id(jobs, job_id, safe_id):
    job = jobs.create_job(job_id=job_id)
    assert job.id == job_id
    assert job.dir == jobs.dir / safe_id


class MockContainer:
    obj = None
    _resource_class = None
    path = '/foo'

    def create_child(self, resource_class, description):
        self.obj = description
        self._resource_class = resource_class
        return MagicMock(spec=RepositoryResource, url='/foo/bar')


@pytest.fixture
def import_file(datadir):
    return datadir / 'item.csv'


def test_import_job_create_resource(import_file, jobs):
    mock_container = MockContainer()
    mock_repo = MagicMock(spec=Repository)
    mock_repo.transaction.return_value = nullcontext()
    mock_repo.__getitem__.return_value = mock_container

    expected_publication_statuses = [
        'Unpublished',
        'Published',
        'UnpublishedHidden',
        'PublishedHidden',
        'Unpublished',
        'Unpublished',
        'Unpublished',
        'UnpublishedHidden',
        'Published',
    ]
    import_job = jobs.create_job(config=ImportConfig(job_id='123', model='Item'))
    for i, stats in enumerate(import_job.run(repo=mock_repo, import_file=import_file.open())):
        assert mock_container.obj is not None
        assert get_publication_status(mock_container.obj) == expected_publication_statuses[i]


def test_import_job_create_resource_publish_all(import_file, jobs):
    mock_container = MockContainer()
    mock_repo = MagicMock(spec=Repository)
    mock_repo.transaction.return_value = nullcontext()
    mock_repo.__getitem__.return_value = mock_container

    expected_publication_statuses = [
        'Published',
        'Published',
        'PublishedHidden',
        'PublishedHidden',
        'Published',
        'Published',
        'Published',
        'PublishedHidden',
        'Published',
    ]
    import_job = jobs.create_job(config=ImportConfig(job_id='123', model='Item'))
    for i, stats in enumerate(import_job.run(repo=mock_repo, publish=True, import_file=import_file.open())):
        assert mock_container.obj is not None
        assert get_publication_status(mock_container.obj) == expected_publication_statuses[i]
