import argparse
import os
import tempfile
from collections import OrderedDict
from io import StringIO
from unittest.mock import MagicMock

import pytest
from rdflib.graph import Graph

import plastron
import plastron.jobs.utils
from plastron.cli.commands.importcommand import Command
from plastron.client import Client, Endpoint
from plastron.jobs import JobConfigError, Row, ImportRun, ImportJob
from plastron.jobs.utils import RepoChangeset
from plastron.models.umd import Item
from plastron.validation import ResourceValidationResult, ValidationError


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


def test_exception_when_no_validation_ruleset(client, monkeypatch):
    # Verifies that the import command throws RuntimeError if item
    # validation throws a NoValidationRulesetException
    mock_job = create_mock_job()
    args = create_args(mock_job.id)
    config = {}

    Command.create_import_job = MagicMock(return_value=mock_job)

    command = Command(config)

    item = MagicMock(Item)
    monkeypatch.setattr(Item, 'validate', MagicMock(side_effect=ValidationError("test")))
    repo_changeset = RepoChangeset(item)

    plastron.jobs.utils.create_repo_changeset = MagicMock(return_value=repo_changeset)

    with pytest.raises(RuntimeError) as excinfo:
        for _ in command.execute(client, args):
            pass

    assert "Unable to run validation" in str(excinfo.value)


def test_invalid_item_added_to_drop_invalid_log(client, monkeypatch):
    # Verifies that the import command adds an invalid item to the
    # drop-invalid log
    mock_job = create_mock_job()
    mock_run = create_mock_run(mock_job)
    monkeypatch.setattr(ImportRun, 'drop_invalid', MagicMock())
    mock_job.new_run = MagicMock(return_value=mock_run)
    args = create_args(mock_job.id)

    invalid_item = create_mock_item(is_valid=False)

    Command.create_import_job = MagicMock(return_value=mock_job)
    config = {}

    command = Command(config)

    repo_changeset = RepoChangeset(invalid_item)

    monkeypatch.setattr(plastron.jobs, 'create_repo_changeset', MagicMock(return_value=repo_changeset))
    monkeypatch.setattr(ImportJob, 'get_source', MagicMock())
    ImportJob.get_source.exists = MagicMock(return_value=True)
    monkeypatch.setattr(ImportJob, 'update_repo', MagicMock(side_effect=RuntimeError))

    plastron.jobs.utils.create_repo_changeset = MagicMock(return_value=repo_changeset)

    for _ in command.execute(client, args):
        pass

    plastron.jobs.create_repo_changeset.assert_called_once()
    ImportJob.update_repo.assert_not_called()
    ImportRun.drop_invalid.assert_called_once()


def test_failed_item_added_to_drop_failed_log(client, monkeypatch):
    # Verifies that the import command adds a failed item to the
    # drop-failed log
    mock_job = create_mock_job()
    mock_run = create_mock_run(mock_job)
    monkeypatch.setattr(ImportRun, 'drop_failed', MagicMock())
    monkeypatch.setattr(ImportRun, 'drop_invalid', MagicMock())
    mock_job.new_run = MagicMock(return_value=mock_run)
    args = create_args(mock_job.id)
    failed_item = create_mock_item(is_valid=True)

    Command.create_import_job = MagicMock(return_value=mock_job)
    config = {}

    command = Command(config)

    repo_changeset = RepoChangeset(failed_item)

    monkeypatch.setattr(plastron.jobs, 'create_repo_changeset', MagicMock(return_value=repo_changeset))
    monkeypatch.setattr(ImportJob, 'get_source', MagicMock())
    ImportJob.get_source.exists = MagicMock(return_value=True)
    monkeypatch.setattr(ImportJob, 'update_repo', MagicMock(side_effect=RuntimeError))

    for _ in command.execute(client, args):
        pass

    plastron.jobs.create_repo_changeset.assert_called_once()
    ImportJob.update_repo.assert_called_once()
    ImportRun.drop_failed.assert_called_once()


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


def create_mock_job():
    """
    Returns an ImportJob with a single row of mock metadata.

    :return: an ImportJob with mock metadata
    """

    job_id = 'test_id'
    test_jobs_dir = tempfile.NamedTemporaryFile()
    mock_job = Command.create_import_job(job_id, test_jobs_dir.name)
    mock_job.save_config = MagicMock()
    mock_job.store_metadata_file = MagicMock()
    mock_metadata = MagicMock()
    row = Row(line_reference='line_reference', row_number=1,
              data=OrderedDict([('FILES', 'test_file'), ('Identifier', 'test-1')]),
              identifier_column='Identifier')
    mock_metadata.__iter__.return_value = [row]

    mock_job.metadata = MagicMock(return_value=mock_metadata)
    mock_job.binaries_location = 'test_binaries_location'
    return mock_job


def create_mock_run(job):
    mock_run = MagicMock(spec=ImportRun)
    mock_run.job = job
    mock_run.start = MagicMock(return_value=mock_run)

    return mock_run


def create_mock_item(is_valid=True):
    """
    Returns a mock Item object

    :param is_valid: True if the item is valid, false otherwise.
                     Defaults to True
    :return: a mock Item object
    """
    mock_item = MagicMock(Item)
    mock_validation_result = MagicMock(ResourceValidationResult)
    mock_validation_result.__bool__ = MagicMock(return_value=is_valid)
    mock_validation_result.is_valid = MagicMock(return_value=is_valid)
    mock_item.validate = MagicMock(return_value=mock_validation_result)
    mock_item.created = False
    return mock_item
