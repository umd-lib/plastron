from tempfile import TemporaryDirectory

import pytest

from plastron.web import create_app


@pytest.fixture
def client(request):
    tmpdir = None
    marker = request.node.get_closest_marker('jobs_dir')
    if marker is not None:
        test_jobs_dir = marker.args[0]
    else:
        # create a new temporary directory that we know will be empty
        tmpdir = TemporaryDirectory()
        test_jobs_dir = tmpdir.name
    app = create_app({'JOBS_DIR': test_jobs_dir})

    with app.test_client() as client:
        yield client

    if tmpdir is not None:
        tmpdir.cleanup()


def test_no_jobs(client):
    response = client.get('/jobs')
    assert response.status_code == 200
    data = response.get_json()
    assert 'jobs' in data
    assert len(data['jobs']) == 0


@pytest.mark.jobs_dir('tests/resources/jobs')
def test_jobs_found(client):
    response = client.get('/jobs')
    assert response.status_code == 200
    data = response.get_json()
    assert 'jobs' in data
    assert len(data['jobs']) == 5


@pytest.mark.jobs_dir('tests/resources/jobs')
def test_job_not_found(client):
    response = client.get('/jobs/FOOBAR')
    assert response.status_code == 404


@pytest.mark.jobs_dir('tests/resources/jobs')
def test_empty_job_dir(client):
    response = client.get('/jobs/noconfigfile')
    assert response.status_code == 404


@pytest.mark.jobs_dir('tests/resources/jobs')
def test_empty_config_file(client):
    response = client.get('/jobs/emptyconfigfile')
    assert response.status_code == 404


@pytest.mark.jobs_dir('tests/resources/jobs')
def test_valid_config_file_no_metadata(client):
    response = client.get('/jobs/nometadata')
    assert response.status_code == 404


@pytest.mark.jobs_dir('tests/resources/jobs')
def test_valid_job(client):
    response = client.get('/jobs/validjob')
    assert response.status_code == 200
    data = response.get_json()
    assert '@id' in data
    assert 'completed' in data
    assert data['completed']['count'] == 0
    assert len(data['completed']['items']) == 0
    assert 'dropped' in data
    assert len(data['dropped']) == 0
    assert 'runs' in data
    assert len(data['runs']) == 0
    assert data['access'] is None
    assert data['binaries_location'] == 'data'
    assert data['container'] == '/dc/2021/2'
    assert data['job_id'] == 'import-20210505143008'
    assert data['member_of'] == 'https://fcrepo.lib.umd.edu/fcrepo/rest/dc/2021/2'
    assert data['model'] == 'Item'
    assert data['total'] == 9


@pytest.mark.jobs_dir('tests/resources/jobs')
def test_valid_completed_job(client):
    response = client.get('/jobs/validcompletedjob')
    assert response.status_code == 200
    data = response.get_json()
    assert '@id' in data
    assert 'completed' in data
    assert data['completed']['count'] == 9
    assert len(data['completed']['items']) == 9
    assert 'dropped' in data
    # 3 keys: "failed", "invalid", "timestamp"
    assert len(data['dropped']) == 3
    assert 'failed' in data['dropped']
    assert 'invalid' in data['dropped']
    assert 'timestamp' in data['dropped']
    assert data['dropped']['timestamp'] == '20210505143008'
    assert 'runs' in data
    assert len(data['runs']) == 1
    assert data['runs'][0] == '20210505143008'
    assert data['access'] is None
    assert data['binaries_location'] == 'data'
    assert data['container'] == '/dc/2021/2'
    assert data['job_id'] == 'import-20210505143008'
    assert data['member_of'] == 'https://fcrepo.lib.umd.edu/fcrepo/rest/dc/2021/2'
    assert data['model'] == 'Item'
    assert data['total'] == 9
