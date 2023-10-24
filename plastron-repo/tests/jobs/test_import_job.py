from plastron.jobs.imports import ImportJob


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
