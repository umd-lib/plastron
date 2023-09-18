import argparse
import os
import tempfile
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

import plastron.jobs.utils
from plastron.cli.commands.importcommand import Command
from plastron.client import Client, Endpoint
from plastron.jobs.importjob import JobConfigError, Row, ImportRun, ImportJob, ImportRow
from plastron.jobs.utils import ImportSpreadsheet, InvalidRow, LineReference
from plastron.models.umd import Item
from plastron.rdfmapping.validation import ValidationResultsDict
from plastron.repo import Repository
from plastron.validation import ValidationError


@pytest.fixture
def client():
    return Client(endpoint=Endpoint(url='http://localhost:8080/fcrepo/rest'))


def test_create_import_job():
    # Verifies "create_import_job" method creates an InputJob with the expected
    # value
    job_id = 'test_job_id'
    jobs_dir = '/test_jobs_dir'

    import_job = Command.create_import_job(job_id, jobs_dir)
    assert import_job.id == job_id
    assert import_job.safe_id == job_id
    assert str(import_job.dir) == os.path.join(jobs_dir, job_id)


def test_cannot_resume_without_job_id(client):
    # Verifies that the import command throws RuntimeError when resuming a
    # job and the job id is not provided
    command = Command()
    args = argparse.Namespace(resume=True, job_id=None)

    with pytest.raises(RuntimeError) as excinfo:
        for _ in command.execute(client, args):
            pass

    assert "Resuming a job requires a job id" in str(excinfo.value)


def test_cannot_resume_without_job_directory(client):
    # Verifies that the import command throws RuntimeError when resuming a
    # job and the directory associated with job id is not found
    jobs_dir = '/nonexistent_directory'
    config = {'JOBS_DIR': jobs_dir}
    command = Command(config)
    args = create_args('test_job_id')
    args.resume = True

    with pytest.raises(RuntimeError) as excinfo:
        for _ in command.execute(client, args):
            pass

    assert "no such job directory" in str(excinfo.value)


def test_cannot_resume_without_config_file(client):
    # Verifies that the import command throws ConfigMissingError when resuming a
    # job and a config file is not found
    job_id = 'test_id'
    args = create_args(job_id)
    args.resume = True

    with tempfile.TemporaryDirectory() as tmpdirname:
        config = {'JOBS_DIR': tmpdirname}

        # Make subdirectory in tmpdirname for job
        job_dir = os.path.join(tmpdirname, job_id)
        os.mkdir(job_dir)

        command = Command(config)

        with pytest.raises(JobConfigError) as excinfo:
            for _ in command.execute(client, args):
                pass

        assert "config.yml is missing" in str(excinfo.value)


def test_model_is_required_unless_resuming(client):
    # Verifies that the import command throws RuntimeError if model
    # is not provided when not resuming
    job_id = 'test_id'
    args = create_args(job_id)
    args.model = None
    config = {}

    command = Command(config)
    with pytest.raises(RuntimeError) as excinfo:
        for _ in command.execute(client, args):
            pass

    assert "A model is required unless resuming an existing job" in str(excinfo.value)


def test_import_file_is_required_unless_resuming(datadir, client):
    # Verifies that the import command throws RuntimeError if an import_file
    # is not provided when not resuming
    job_id = 'test_id'
    args = create_args(job_id)
    args.import_file = None
    config = {'JOBS_DIR': datadir}

    command = Command(config)
    with pytest.raises(RuntimeError) as excinfo:
        for _ in command.execute(client, args):
            pass

    assert "An import file is required unless resuming an existing job" in str(excinfo.value)


@patch('plastron.cli.commands.importcommand.ImportJob')
def test_exception_when_no_validation_ruleset(MockImportJob, client, monkeypatch, datadir):
    # Verifies that the import command throws RuntimeError if item
    # validation throws a NoValidationRulesetException
    mock_item = MagicMock(spec=Item)
    mock_item.validate.side_effect = ValidationError('Unable to run validation')
    mock_run = MagicMock(spec=ImportRun)
    mock_run.start.return_value = mock_run
    mock_spreadsheet = MagicMock(spec=ImportSpreadsheet)
    mock_spreadsheet.has_binaries = False
    mock_spreadsheet.total = 1
    row = Row(
        data={'id': 'foo', 'FILES': ''},
        line_reference=LineReference('foo', 1),
        spreadsheet=mock_spreadsheet,
        identifier_column='id',
        row_number=1
    )
    monkeypatch.setattr(row, 'get_object', lambda _repo: mock_item)
    mock_spreadsheet.rows = MagicMock(return_value=[row])
    job = ImportJob(job_id='123', jobs_dir=datadir)
    import_row = ImportRow(job=job, repo=MagicMock(spec=Repository), row=row)
    monkeypatch.setattr(job, 'new_run', lambda: mock_run)
    monkeypatch.setattr(job, 'get_metadata', lambda: mock_spreadsheet)
    monkeypatch.setattr(job, 'get_import_row', lambda _repo, _row: import_row)
    MockImportJob.return_value = job

    args = create_args(job.id)
    command = Command(config={})

    with pytest.raises(RuntimeError) as excinfo:
        for _ in command.execute(client, args):
            pass

    assert "Unable to run validation" in str(excinfo.value)


@patch('plastron.cli.commands.importcommand.ImportJob')
def test_invalid_item_added_to_drop_invalid_log(MockImportJob, datadir, client, monkeypatch):
    # Verifies that the import command adds an invalid item to the
    # drop-invalid log
    # mock the ImportRun so that we can see if the drop_invalid method was called
    mock_run = MagicMock(spec=ImportRun)
    mock_run.start.return_value = mock_run
    # mock a spreadsheet with get_rows() that yields an InvalidRow
    mock_spreadsheet = MagicMock(spec=ImportSpreadsheet)
    mock_spreadsheet.has_binaries = False
    mock_spreadsheet.total = 1
    mock_spreadsheet.rows = MagicMock(return_value=[InvalidRow(LineReference('foo', 1), 'test')])
    job = ImportJob(job_id='123', jobs_dir=datadir)
    monkeypatch.setattr(job, 'new_run', lambda: mock_run)
    monkeypatch.setattr(job, 'get_metadata', lambda: mock_spreadsheet)
    MockImportJob.return_value = job

    args = create_args(job.id)
    command = Command({})

    for _ in command.execute(client, args):
        pass

    mock_run.drop_invalid.assert_called_once()
    mock_run.drop_failed.assert_not_called()


@patch('plastron.jobs.importjob.ImportRow')
@patch('plastron.cli.commands.importcommand.ImportJob')
def test_failed_item_added_to_drop_failed_log(MockImportJob, MockImportRow, client, monkeypatch, datadir):
    # Verifies that the import command adds a failed item to the
    # drop-failed log
    # mock the ImportRun so that we can see if the drop_failed method was called
    mock_run = MagicMock(spec=ImportRun)
    mock_run.start.return_value = mock_run
    mock_row = MagicMock(spec=Row, data=['foo'], line_reference=LineReference('foo', 1))
    mock_spreadsheet = MagicMock(spec=ImportSpreadsheet)
    mock_spreadsheet.has_binaries = False
    mock_spreadsheet.total = 1
    mock_spreadsheet.rows = MagicMock(return_value=[mock_row])
    mock_import_row = MagicMock(spec=ImportRow)
    mock_import_row.validate_item.return_value = ValidationResultsDict()
    mock_import_row.update_repo.side_effect = plastron.jobs.importjob.JobError('test')
    mock_import_row.item = 'Foobar'
    job = ImportJob(job_id='123', jobs_dir=datadir)
    monkeypatch.setattr(job, 'new_run', lambda: mock_run)
    monkeypatch.setattr(job, 'get_metadata', lambda: mock_spreadsheet)
    MockImportJob.return_value = job
    MockImportRow.return_value = mock_import_row

    args = create_args(job.id)
    command = Command({})

    for _ in command.execute(client, args):
        pass

    mock_run.drop_failed.assert_called_once()
    mock_run.drop_invalid.assert_not_called()


def create_args(job_id):
    """
    Returns an argparse.Namespace object suitable for use in testing.

    Individual tests should override attributes as needed to support
    their scenario.

    :param job_id: the job id to use
    :return: a configured argparse.Namespace object
    """
    return argparse.Namespace(
        resume=False, job_id=job_id,
        model='Item',
        access=None,
        member_of="test",
        container="test_container",
        binaries_location="test_binaries_location",
        template_file=None,
        import_file=StringIO('Title,Identifier\nfoobar,123'),
        percentage=None,
        validate_only=False,
        limit=None,
    )
