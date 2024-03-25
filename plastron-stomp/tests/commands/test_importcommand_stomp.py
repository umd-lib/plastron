from unittest.mock import MagicMock, patch, ANY

import pytest

from plastron.context import PlastronContext
from plastron.jobs import Jobs
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
@patch.object(Jobs, "create_job")
def test_publish(create_job, headers, expected_args):
    # Mock the job object and its run method
    mock_job = MagicMock()
    mock_job.run.return_value = {}
    create_job.return_value = mock_job

    message = PlastronCommandMessage(headers=headers, body=message_body)
    mock_context = MagicMock(
        spec=PlastronContext,
        repo=mock_repo,
        config={'COMMANDS': {'IMPORT': {'JOBS_DIR': 'some_jobs_dir'}}}
    )

    # Call the importcommand function with the mock repo, config, and message
    importcommand(mock_context, message)

    # Assert that the job.run method was called with correct value for publish
    mock_job.run.assert_called_with(**expected_args)
