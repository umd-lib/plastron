from unittest.mock import MagicMock, patch, ANY

import pytest
from pytest import raises

from plastron.jobs.imports import ImportJobs
from plastron.repo import Repository
from plastron.stomp.commands.importcommand import importcommand
from plastron.stomp.messages import PlastronCommandMessage


@pytest.fixture
def message_body():
    return '{"uris": ["test"], "sparql_update": "" }'

@pytest.fixture
def mock_repo():
    return MagicMock(spec=Repository)

@pytest.mark.parametrize(
    ('headers', 'expected_args'),
    [
        (
            # headers
            {
                'PlastronJobId': 'test',
                'PlastronCommand': 'update',
                'PlastronArg-dry-run': 'False',
                'PlastronArg-no-transactions': 'True',
                'PlastronArg-validate-only': 'False',
                'PlastronArg-publish': 'False'
            },
            # expected args
            {
                'repo': mock_repo,
                'import_file': ANY,
                'limit': None,
                'percentage': None,
                'validate_only': False,
                'publish': False,
            },
        ),
        (
            # headers
            {
                'PlastronJobId': 'test',
                'PlastronCommand': 'update',
                'PlastronArg-dry-run': 'False',
                'PlastronArg-no-transactions': 'True',
                'PlastronArg-validate-only': 'True',
                'PlastronArg-publish': 'True'
            },
            # expected args
            {
                'repo': mock_repo,
                'import_file': ANY,
                'limit': None,
                'percentage': None,
                'validate_only': True,
                'publish': True,
            },
        ),
    ],
)
@patch.object(ImportJobs, "create_job") # Mock the create_job method in ImportJobs class
def test_publish(create_job, headers, expected_args):
    # Mock the job object and its run method
    mock_job = MagicMock()
    mock_job.run.return_value = {}  # Return value can be adjusted based on your needs
    create_job.return_value = mock_job

    message = PlastronCommandMessage(headers=headers, body=message_body)

    # Call the importcommand function with the mock repo, config, and message
    result = importcommand(mock_repo, {'JOBS_DIR': 'some_jobs_dir'}, message)

    # Assert that the job.run method was called with correct value for publish
    mock_job.run.assert_called_with(**expected_args)
