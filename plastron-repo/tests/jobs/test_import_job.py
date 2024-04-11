from contextlib import nullcontext
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from plastron.repo.publish import get_publication_status
from plastron.jobs import JobConfigError, Jobs
from plastron.jobs.importjob import ImportConfig, ImportJob
from plastron.repo import Repository, RepositoryResource


@pytest.fixture
def jobs_dir(datadir) -> Path:
    return datadir


@pytest.fixture
def jobs(jobs_dir) -> Jobs:
    return Jobs(jobs_dir)


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
    job = jobs.create_job(ImportJob, job_id=job_id)
    assert job.id == job_id
    assert job.dir == jobs.dir / safe_id


@pytest.mark.parametrize(
    ('job_id', 'expected_message'),
    [
        ('no-config', 'is missing'),
        ('empty-config', 'is empty'),
    ]
)
def test_job_config_errors(jobs, job_id, expected_message):
    with pytest.raises(JobConfigError) as exc_info:
        jobs.get_job(ImportJob, job_id)

    assert expected_message in str(exc_info.value)


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
    import_job = jobs.create_job(ImportJob, config=ImportConfig(job_id='123', model='Item'))
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
    import_job = jobs.create_job(ImportJob, config=ImportConfig(job_id='123', model='Item'))
    for i, stats in enumerate(import_job.run(repo=mock_repo, publish=True, import_file=import_file.open())):
        assert mock_container.obj is not None
        assert get_publication_status(mock_container.obj) == expected_publication_statuses[i]


def test_config_read_none_string_as_none(jobs):
    # ensure that when reading improperly serialized config files,
    # the string "None" gets treated as the value None
    job = jobs.get_job(job_class=ImportJob, job_id='bad_access')
    assert job.access is None


def test_config_read_null_as_none(jobs):
    # ensure that when reading the config file, "null" is converted
    # to the value None
    job = jobs.get_job(job_class=ImportJob, job_id='null_access')
    assert job.access is None


def test_config_read_empty_as_none(jobs):
    # ensure that when reading the config file, an empty value
    # is converted to the value None
    job = jobs.get_job(job_class=ImportJob, job_id='empty_access')
    assert job.access is None


def test_config_write_none_as_null(jobs):
    # ensure that when writing the config file, values of None
    # are serialized as a YAML null; see https://yaml.org/type/null.html
    # for details on YAML's treatment of null values
    job = jobs.create_job(
        job_class=ImportJob,
        job_id='fixed_saved',
        config=ImportConfig(job_id='fixed_saved', access=None),
    )
    contents = job.config_filename.read_text()
    assert 'access: None\n' not in contents
    assert 'access: null\n' in contents


def test_import_job_validation_fails_for_job_with_files_column_and_file_missing(jobs, datadir):
    """
    Verifies the import validation fails when
    - The CSV file contains an "ITEM_FILES" column that contains non-empty values
    - A "binaries_location" parameter is provided
    - The file is not found
    """
    import_file = datadir / 'item_with_file_in_item_files_column.csv'
    binaries_location = 'test_binaries_location'
    mock_repo = MagicMock(spec=Repository)

    import_job = jobs.create_job(ImportJob, config=ImportConfig(job_id='456', model='Item', binaries_location=binaries_location))
    with pytest.raises(StopIteration) as exc_info:
      next(import_job.run(repo=mock_repo, validate_only=True, import_file=import_file.open()))

    return_value = exc_info.value.value
    assert return_value['type'] == 'validate_failed'


def test_import_job_raises_runtime_error_job_with_files_and_no_binaries_location(jobs, datadir):
    """
    Verifies that the import job raises a RuntimeError when:
    - The CSV file contains a "FILES" or "ITEM_FILES" column that contains non-empty values
    - A "binaries_location" parameter is NOT provided
    """
    import_file = datadir / 'item_with_file_in_item_files_column.csv'
    mock_repo = MagicMock(spec=Repository)

    import_job = jobs.create_job(ImportJob, config=ImportConfig(job_id='456', model='Item'))
    with pytest.raises(RuntimeError) as exc_info:
      next(import_job.run(repo=mock_repo, validate_only=True, import_file=import_file.open()))

    expected_message = 'Must specify --binaries-location if the metadata has a FILES and/or ITEM_FILES column'
    assert expected_message == str(exc_info.value)


def test_import_job_validation_succeeds_for_job_with_files_column_and_file_exists(jobs, datadir, tmpdir):
    """
    Verifies the import validation fails when
    - The CSV file contains an "ITEM_FILES" column that contains non-empty values
    - A "binaries_location" parameter is provided
    - The indicated file is found
    """
    import_file = datadir / 'item_with_file_in_item_files_column.csv'
    binaries_location = tmpdir.mkdir('test_binary_location')
    temp_binary_file = binaries_location / 'test_file.tif'
    temp_binary_file.write('Test file')

    mock_repo = MagicMock(spec=Repository)

    import_job = jobs.create_job(ImportJob, config=ImportConfig(job_id='456', model='Item', binaries_location=str(binaries_location)))
    with pytest.raises(StopIteration) as exc_info:
      next(import_job.run(repo=mock_repo, validate_only=True, import_file=import_file.open()))

    return_value = exc_info.value.value
    assert return_value['type'] == 'validate_success'
