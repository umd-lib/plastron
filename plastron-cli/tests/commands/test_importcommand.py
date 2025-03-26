import argparse
import os
import tempfile
from csv import DictReader
from io import StringIO
from uuid import uuid4

import pytest

from plastron.cli.commands.importcommand import Command
from plastron.jobs import JobConfigError, JobNotFoundError


def test_cannot_resume_without_job_id(plastron_context):
    # Verifies that the import command throws RuntimeError when resuming a
    # job and the job id is not provided
    args = create_args(job_id=None, resume=True)
    plastron_context.args = args
    command = Command(context=plastron_context)

    with pytest.raises(RuntimeError) as excinfo:
        for _ in command(args):
            pass

    assert "Resuming a job requires a job id" in str(excinfo.value)


def test_cannot_resume_without_job_directory(plastron_context):
    # Verifies that the import command throws RuntimeError when resuming a
    # job and the directory associated with job id is not found
    jobs_dir = '/nonexistent_directory'
    plastron_context.config.update({'COMMANDS': {'IMPORT': {'JOBS_DIR': jobs_dir}}})
    args = create_args(resume=True)
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
    args = create_args(job_id=job_id, resume=True)
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
    args = create_args(model=None, resume=False)
    plastron_context.args = args

    command = Command(context=plastron_context)
    with pytest.raises(RuntimeError) as excinfo:
        for _ in command(args):
            pass

    assert "A model is required unless resuming an existing job" in str(excinfo.value)


def test_import_file_is_required_unless_resuming(datadir, plastron_context):
    # Verifies that the import command throws RuntimeError if an import_file
    # is not provided when not resuming
    args = create_args(import_file=None, resume=False)
    plastron_context.args = args

    command = Command(context=plastron_context)
    with pytest.raises(RuntimeError) as excinfo:
        for _ in command(args):
            pass

    assert "An import file is required unless resuming an existing job" in str(excinfo.value)


def test_container_is_required_unless_resuming(datadir, plastron_context):
    # Verifies that the import command throws RuntimeError if an import_file
    # is not provided when not resuming
    args = create_args(container=None, resume=False)
    plastron_context.args = args

    command = Command(context=plastron_context)
    with pytest.raises(RuntimeError) as excinfo:
        for _ in command(args):
            pass

    assert "A container is required unless resuming an existing job" in str(excinfo.value)


@pytest.mark.parametrize(
    ('model_name', 'expected_header_fields'),
    [
        (
            'Item',
            [
                'Object Type', 'Identifier', 'Rights Statement', 'Title',
                'Format', 'Archival Collection', 'Presentation Set', 'Date',
                'Description', 'Alternate Title', 'Creator', 'Creator URI',
                'Contributor', 'Contributor URI', 'Publisher', 'Publisher URI',
                'Location', 'Extent', 'Subject', 'Language', 'Rights Holder',
                'Terms of Use', 'Copyright Notice', 'Collection Information',
                'Accession Number', 'Handle', 'FILES', 'ITEM_FILES'
            ]
        ),
        (
            'Letter',
            [
                'Title', 'Rights Holder', 'Extent', 'Bibliographic Citation',
                'Description', 'Language', 'Date', 'Resource Type', 'Rights',
                'Terms of Use', 'Copyright Notice', 'Subject', 'Location',
                'Longitude', 'Latitude', 'Archival Collection', 'Handle/Link',
                'Identifier', 'Recipient', 'Author', 'Handle',
                'Presentation Set', 'FILES', 'ITEM_FILES'
            ]
        ),
        (
            'Poster',
            [
                'Title', 'Alternate Title', 'Publisher', 'Collection',
                'Format', 'Resource Type', 'Date', 'Language', 'Description',
                'Extent', 'Issue', 'Identifier/Call Number', 'Location',
                'Longitude', 'Latitude', 'Subject', 'Rights', 'Terms of Use',
                'Copyright Notice', 'Identifier', 'Handle', 'Presentation Set',
                'FILES', 'ITEM_FILES'
            ]
        ),
        (
            'Issue',
            [
                'Title', 'Date', 'Volume', 'Issue', 'Edition', 'Handle',
                'Presentation Set', 'Copyright Notice', 'Terms of Use',
                'FILES', 'ITEM_FILES'
            ]
        ),

    ]
)
def test_make_template(plastron_context, model_name, expected_header_fields):
    template_file = tempfile.NamedTemporaryFile(delete=False)
    with open(template_file.name, 'w') as template_file:
        args = create_args(model=model_name, template_file=template_file)
        plastron_context.args = args
        command = Command(context=plastron_context)
        command(args)

    with open(template_file.name, 'r') as template_file:
        template_file.seek(0)

        csv_reader = DictReader(template_file)
        header_fields = csv_reader.fieldnames
        os.unlink(template_file.name)

        assert header_fields == expected_header_fields


def create_args(**kwargs):
    """
    Returns an argparse.Namespace object suitable for use in testing.

    Individual tests should override attributes as needed to support
    their scenario.

    :param job_id: the job id to use
    :return: a configured argparse.Namespace object
    """
    params = {
        'job_id': str(uuid4()),
        'delegated_user': None,
        'resume': False,
        'model': 'Item',
        'access': None,
        'member_of': "test",
        'container': "test_container",
        'binaries_location': "test_binaries_location",
        'template_file': None,
        'import_file': StringIO('Title,Identifier\nfoobar,123'),
        'percentage': None,
        'validate_only': False,
        'limit': None,
    }
    params.update(kwargs)
    return argparse.Namespace(**params)
