from contextlib import nullcontext
from unittest.mock import MagicMock

from plastron.cli.commands.publish import get_publication_status
from plastron.jobs.imports import ImportJob
from plastron.repo import Repository, RepositoryResource


def test_safe_job_id():
    job = ImportJob('foo', '/tmp')
    assert job.id == 'foo'
    assert job.safe_id == 'foo'


def test_job_id_with_slashes():
    job = ImportJob('foo/bar', '/tmp')
    assert job.id == 'foo/bar'
    assert job.safe_id == 'foo%2Fbar'


def test_uri_as_job_id():
    job = ImportJob('http://localhost:3000/import-jobs/17', '/tmp')
    assert job.id == 'http://localhost:3000/import-jobs/17'
    assert job.safe_id == 'http%3A%2F%2Flocalhost%3A3000%2Fimport-jobs%2F17'


def test_import_job_create_resource(datadir):
    class MockContainer:
        obj = None
        _resource_class = None
        path = '/foo'

        def create_child(self, resource_class, description):
            self.obj = description
            self._resource_class = resource_class
            return MagicMock(spec=RepositoryResource, url='/foo/bar')

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
    import_job = ImportJob('123', datadir)
    import_file = datadir / 'item.csv'
    for i, stats in enumerate(import_job.start(repo=mock_repo, model='Item', import_file=import_file.open())):
        assert mock_container.obj is not None
        assert get_publication_status(mock_container.obj) == expected_publication_statuses[i]
