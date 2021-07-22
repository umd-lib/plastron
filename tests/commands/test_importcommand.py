import argparse
import os
import pytest
import tempfile
from collections import OrderedDict
from plastron.commands.importcommand import Command, RepoChangeset
from plastron.exceptions import FailureException, NoValidationRulesetException
from plastron.jobs import Row
from plastron.models.umd import Item
from plastron.validation import ResourceValidationResult
from rdflib.graph import Graph
from unittest.mock import MagicMock


def test_create_import_job():
    # Verifies "create_import_job" method creates an InputJob with the expected
    # value
    job_id = 'test_job_id'
    jobs_dir = '/test_jobs_dir'

    import_job = Command.create_import_job(job_id, jobs_dir)
    assert import_job.id == job_id
    assert import_job.safe_id == job_id
    assert import_job.dir == os.path.join(jobs_dir, job_id)


def test_cannot_resume_without_job_id():
    # Verifies that the import command throws FailureException when resuming a
    # job and the job id is not provided
    command = Command()
    args = argparse.Namespace(resume=True, job_id=None)
    repo = None

    with pytest.raises(FailureException) as excinfo:
        for _ in command.execute(repo, args):
            pass

    assert "Resuming a job requires a job id" in str(excinfo.value)


def test_cannot_resume_without_job_directory():
    # Verifies that the import command throws FailureException when resuming a
    # job and the directory associated with job id is not found
    jobs_dir = '/nonexistent_directory'
    config = {'JOBS_DIR': jobs_dir}
    command = Command(config)
    args = create_args('test_job_id')
    args.resume = True
    repo = None

    with pytest.raises(FailureException) as excinfo:
        for _ in command.execute(repo, args):
            pass

    assert "no such job directory found" in str(excinfo.value)


def test_cannot_resume_without_config_file():
    # Verifies that the import command throws FailureException when resuming a
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
        repo = None

        with pytest.raises(FailureException) as excinfo:
            for _ in command.execute(repo, args):
                pass

        assert "no config.yml found" in str(excinfo.value)


def test_model_is_required_unless_resuming():
    # Verifies that the import command throws FailureException if model
    # is not provided when not resuming
    job_id = 'test_id'
    args = create_args(job_id)
    args.model = None
    config = {}

    command = Command(config)
    repo = None
    with pytest.raises(FailureException) as excinfo:
        for _ in command.execute(repo, args):
            pass

    assert "A model is required unless resuming an existing job" in str(excinfo.value)


def test_import_file_is_required_unless_resuming():
    # Verifies that the import command throws FailureException if an import_file
    # is not provided when not resuming
    job_id = 'test_id'
    args = create_args(job_id)
    args.import_file = None
    config = {}

    command = Command(config)
    repo = None
    with pytest.raises(FailureException) as excinfo:
        for _ in command.execute(repo, args):
            pass

    assert "An import file is required unless resuming an existing job" in str(excinfo.value)


def test_exception_when_no_validation_ruleset():
    # Verifies that the import command throws FailureException if item
    # validation throws a NoValidationRulesetException
    mock_job = create_mock_job()
    args = create_args(mock_job.id)
    config = {}

    Command.create_import_job = MagicMock(return_value=mock_job)

    command = Command(config)
    repo = None

    item = MagicMock(Item)
    item.validate = MagicMock(side_effect=NoValidationRulesetException("test"))
    repo_changeset = RepoChangeset(item, None, None)

    command.create_repo_changeset = MagicMock(return_value=repo_changeset)

    with pytest.raises(FailureException) as excinfo:
        for _ in command.execute(repo, args):
            pass

    assert "Unable to run validation" in str(excinfo.value)


def test_invalid_item_added_to_drop_log():
    # Verifies that the import command adds an invalid item to the drop log
    mock_job = create_mock_job()
    args = create_args(mock_job.id)

    invalid_item = create_mock_item(is_valid=False)

    Command.create_import_job = MagicMock(return_value=mock_job)
    config = {}

    command = Command(config)
    repo = None

    repo_changeset = RepoChangeset(invalid_item, None, None)

    command.create_repo_changeset = MagicMock(return_value=repo_changeset)
    command.update_repo = MagicMock()
    mock_job.drop = MagicMock()

    for _ in command.execute(repo, args):
        pass

    command.create_repo_changeset.assert_called_once()
    command.update_repo.assert_not_called()
    mock_job.drop.assert_called_once()


def test_failed_item_added_to_drop_log():
    # Verifies that the import command adds a failed item to the drop log
    mock_job = create_mock_job()
    args = create_args(mock_job.id)
    failed_item = create_mock_item(is_valid=True)

    Command.create_import_job = MagicMock(return_value=mock_job)
    config = {}

    command = Command(config)
    repo = None

    repo_changeset = RepoChangeset(failed_item, Graph(), Graph())

    command.create_repo_changeset = MagicMock(return_value=repo_changeset)
    command.get_source = MagicMock()
    command.get_source.exists = MagicMock(return_value=True)
    command.update_repo = MagicMock(side_effect=FailureException)

    mock_job.drop = MagicMock()

    for _ in command.execute(repo, args):
        pass

    command.create_repo_changeset.assert_called_once()
    command.update_repo.assert_called_once()
    mock_job.drop.assert_called_once()


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
        import_file='test_import_file',
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
    mock_job = Command.create_import_job(job_id, 'test_jobs_dir')
    mock_job.save_config = MagicMock()
    mock_job.store_metadata_file = MagicMock()
    mock_metadata = MagicMock()
    row = Row(line_reference='line_reference', row_number=1,
              data=OrderedDict([{'FILES', 'test_file'}, {'Identifier', 'test-1'}]),
              identifier_column='Identifier')
    mock_metadata.__iter__.return_value = [row]

    mock_job.metadata = MagicMock(return_value=mock_metadata)
    mock_job.binaries_location = 'test_binaries_location'
    return mock_job


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
    return mock_item
