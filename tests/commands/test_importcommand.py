import argparse
import os
import pytest
import tempfile
from plastron.exceptions import FailureException
from plastron.commands.importcommand import Command


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
    args = argparse.Namespace(resume=True, job_id='test_job_id')
    repo = None

    with pytest.raises(FailureException) as excinfo:
        for _ in command.execute(repo, args):
            pass

    assert "no such job directory found" in str(excinfo.value)


def test_cannot_resume_without_config_file():
    # Verifies that the import command throws FailureException when resuming a
    # job and a config file is not found
    job_id = 'test_id'
    args = argparse.Namespace(resume=True, job_id=job_id)

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
    args = argparse.Namespace(resume=False, job_id=job_id, model=None)
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
    args = argparse.Namespace(
        resume=False, job_id=job_id, model='Item', access=None,
        member_of="test",
        container="test_container",
        binaries_location="test_binaries_location",
        template_file=None,
        import_file=None
    )
    config = {}

    command = Command(config)
    repo = None
    with pytest.raises(FailureException) as excinfo:
        for _ in command.execute(repo, args):
            pass

    assert "An import file is required unless resuming an existing job" in str(excinfo.value)
