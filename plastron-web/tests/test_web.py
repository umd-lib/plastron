import pytest

from plastron.web import create_app


@pytest.fixture
def app_client(config_file_path, datadir, monkeypatch):
    def _create_app_client(jobs_dir):
        test_jobs_dir = datadir / jobs_dir
        monkeypatch.setenv('JOBS_DIR', str(test_jobs_dir))
        app = create_app(config_file_path)

        return app.test_client()

    return _create_app_client


def test_no_jobs(app_client):
    response = app_client('jobsempty').get('/jobs')
    assert response.status_code == 200
    data = response.get_json()
    assert 'jobs' in data
    assert len(data['jobs']) == 0


def test_jobs_found(app_client):
    response = app_client('jobs').get('/jobs')
    assert response.status_code == 200
    data = response.get_json()
    assert 'jobs' in data
    assert len(data['jobs']) == 5


def test_job_not_found(app_client):
    response = app_client('jobs').get('/jobs/FOOBAR')
    assert response.status_code == 404


def test_empty_job_dir(app_client):
    response = app_client('jobs').get('/jobs/noconfigfile')
    assert response.status_code == 404


def test_empty_config_file(app_client):
    response = app_client('jobs').get('/jobs/emptyconfigfile')
    assert response.status_code == 404


def test_valid_config_file_no_metadata(app_client):
    response = app_client('jobs').get('/jobs/nometadata')
    assert response.status_code == 404


def test_valid_job(app_client):
    response = app_client('jobs').get('/jobs/validjob')
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


def test_valid_completed_job(app_client):
    response = app_client('jobs').get('/jobs/validcompletedjob')
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
