import argparse
import os
import tempfile
from io import StringIO

import pytest

from plastron.cli.commands.importcommand import Command
from plastron.client import Client, Endpoint
from plastron.jobs import JobConfigError, JobNotFoundError


@pytest.fixture
def client():
    return Client(endpoint=Endpoint(url='http://localhost:8080/fcrepo/rest'))


def test_cannot_resume_without_job_id(client):
    # Verifies that the import command throws RuntimeError when resuming a
    # job and the job id is not provided
    command = Command()
    args = argparse.Namespace(resume=True, job_id=None)

    with pytest.raises(RuntimeError) as excinfo:
        for _ in command(args):
            pass

    assert "Resuming a job requires a job id" in str(excinfo.value)


def test_cannot_resume_without_job_directory(plastron_context):
    # Verifies that the import command throws RuntimeError when resuming a
    # job and the directory associated with job id is not found
    jobs_dir = '/nonexistent_directory'
    plastron_context.config.update({'COMMANDS': {'IMPORT': {'JOBS_DIR': jobs_dir}}})
    args = create_args('test_job_id')
    args.resume = True
    plastron_context.args = args
    command = Command(context=plastron_context)

    with pytest.raises(JobNotFoundError) as excinfo:
        for _ in command(args):
            pass

    assert "does not exist" in str(excinfo.value)


def test_cannot_resume_without_config_file(plastron_context):
    # Verifies that the import command throws ConfigMissingError when resuming a
    # job and a config file is not found
    job_id = 'test_id'
    args = create_args(job_id)
    args.resume = True
    plastron_context.args = args

    with tempfile.TemporaryDirectory() as tmpdirname:
        plastron_context.config.update({'COMMANDS': {'IMPORT': {'JOBS_DIR': tmpdirname}}})

        # Make subdirectory in tmpdirname for job
        job_dir = os.path.join(tmpdirname, job_id)
        os.mkdir(job_dir)

        command = Command(context=plastron_context)

        with pytest.raises(JobConfigError) as excinfo:
            for _ in command(args):
                pass

        assert "config.yml is missing" in str(excinfo.value)


def test_model_is_required_unless_resuming(plastron_context):
    # Verifies that the import command throws RuntimeError if model
    # is not provided when not resuming
    job_id = 'test_id'
    args = create_args(job_id)
    args.model = None
    plastron_context.args = args

    command = Command(context=plastron_context)
    with pytest.raises(RuntimeError) as excinfo:
        for _ in command(args):
            pass

    assert "A model is required unless resuming an existing job" in str(excinfo.value)


def test_import_file_is_required_unless_resuming(datadir, plastron_context):
    # Verifies that the import command throws RuntimeError if an import_file
    # is not provided when not resuming
    job_id = 'test_id'
    args = create_args(job_id)
    args.import_file = None
    plastron_context.args = args
    plastron_context.config.update({'COMMANDS': {'IMPORT': {'JOBS_DIR': datadir}}})

    command = Command(context=plastron_context)
    with pytest.raises(RuntimeError) as excinfo:
        for _ in command(args):
            pass

    assert "An import file is required unless resuming an existing job" in str(excinfo.value)


def create_args(job_id):
    """
    Returns an argparse.Namespace object suitable for use in testing.

    Individual tests should override attributes as needed to support
    their scenario.

    :param job_id: the job id to use
    :return: a configured argparse.Namespace object
    """
    return argparse.Namespace(
        job_id=job_id,
        delegated_user=None,
        resume=False,
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
