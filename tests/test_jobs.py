import pytest

from plastron.jobs import ImportJob


@pytest.fixture
def jobs_dir(datadir):
    return datadir / 'jobs'


def test_config_read_none_string_as_none(jobs_dir):
    # ensure that when reading improperly serialized config files,
    # the string "None" gets treated as the value None
    job = ImportJob(job_id='bad_access', jobs_dir=jobs_dir)
    job.load_config()
    assert job.access is None


def test_config_read_null_as_none(jobs_dir):
    # ensure that when reading the config file, "null" is converted
    # to the value None
    job = ImportJob(job_id='null_access', jobs_dir=jobs_dir)
    job.load_config()
    assert job.access is None


def test_config_read_empty_as_none(jobs_dir):
    # ensure that when reading the config file, an empty value
    # is converted to the value None
    job = ImportJob(job_id='empty_access', jobs_dir=jobs_dir)
    job.load_config()
    assert job.access is None


def test_config_write_none_as_null(jobs_dir):
    # ensure that when writing the config file, values of None
    # are serialized as a YAML null; see https://yaml.org/type/null.html
    # for details on YAML's treatment of null values
    job = ImportJob(job_id='fixed_saved', jobs_dir=jobs_dir)
    job.save_config({'access': None})
    contents = job.config_filename.read_text()
    assert 'access: None\n' not in contents
    assert 'access: null\n' in contents
