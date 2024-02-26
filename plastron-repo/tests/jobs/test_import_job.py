from pathlib import Path

import pytest

from plastron.jobs import JobConfigError
from plastron.jobs.importjob import ImportJobs, ImportConfig


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


@pytest.mark.parametrize(
    ('job_id', 'expected_message'),
    [
        ('no-config', 'is missing'),
        ('empty-config', 'is empty'),
    ]
)
def test_job_config_errors(jobs, job_id, expected_message):
    with pytest.raises(JobConfigError) as exc_info:
        jobs.get_job(job_id)

    assert expected_message in str(exc_info.value)


def test_config_read_none_string_as_none(jobs):
    # ensure that when reading improperly serialized config files,
    # the string "None" gets treated as the value None
    job = jobs.get_job(job_id='bad_access')
    assert job.access is None


def test_config_read_null_as_none(jobs):
    # ensure that when reading the config file, "null" is converted
    # to the value None
    job = jobs.get_job(job_id='null_access')
    assert job.access is None


def test_config_read_empty_as_none(jobs):
    # ensure that when reading the config file, an empty value
    # is converted to the value None
    job = jobs.get_job(job_id='empty_access')
    assert job.access is None


def test_config_write_none_as_null(jobs):
    # ensure that when writing the config file, values of None
    # are serialized as a YAML null; see https://yaml.org/type/null.html
    # for details on YAML's treatment of null values
    job = jobs.create_job(job_id='fixed_saved', config=ImportConfig(job_id='fixed_saved', access=None))
    contents = job.config_filename.read_text()
    assert 'access: None\n' not in contents
    assert 'access: null\n' in contents
