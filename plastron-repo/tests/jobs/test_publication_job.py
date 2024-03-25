from typing import Callable
from unittest.mock import MagicMock

import pytest

from plastron.context import PlastronContext
from plastron.handles import Handle
from plastron.jobs.publicationjob import PublicationJob, PublicationAction
from plastron.repo import Repository, RepositoryError
from plastron.repo.publish import PublishableResource


class JobRunner:
    def __init__(self, job, callback: Callable = None):
        self.job = job
        self.callback = callback
        self.result = None

    def run(self, *args, **kwargs):
        for status in self._run(*args, **kwargs):
            if self.callback is not None:
                self.callback(status)
        return self.result

    def _run(self, *args, **kwargs):
        self.result = yield from self.job.run(*args, **kwargs)


@pytest.mark.parametrize(
    ('action', 'with_errors', 'expected_status', 'expected_done', 'expected_errors'),
    [
        (PublicationAction.PUBLISH, False, 'publish_complete', 2, 0),
        (PublicationAction.UNPUBLISH, False, 'unpublish_complete', 2, 0),
        (PublicationAction.PUBLISH, True, 'publish_incomplete', 1, 1),
        (PublicationAction.UNPUBLISH, True, 'unpublish_incomplete', 1, 1),
        ('bad_action', True, 'error', 0, 2),
    ]
)
def test_publication_job(action, with_errors, expected_status, expected_done, expected_errors):
    mock_resource = MagicMock(spec=PublishableResource)
    mock_resource.read.return_value = mock_resource
    mock_handle = MagicMock(spec=Handle)
    if action == PublicationAction.PUBLISH:
        if with_errors:
            mock_resource.publish.side_effect = [mock_handle, RepositoryError]
        else:
            mock_resource.publish.return_value = mock_handle
    elif action == PublicationAction.UNPUBLISH:
        if with_errors:
            mock_resource.unpublish.side_effect = [mock_handle, RepositoryError]
        else:
            mock_resource.unpublish.return_value = mock_handle
    mock_repo = MagicMock(spec=Repository)
    mock_repo.__getitem__.return_value = mock_resource
    mock_context = MagicMock(spec=PlastronContext, repo=mock_repo)

    job = PublicationJob(
        context=mock_context,
        action=action,
        uris=[
            'http://fcrepo-local:8080/fcrepo/rest/foo',
            'http://fcrepo-local:8080/fcrepo/rest/bar',
        ]
    )

    def check_status(status):
        assert 'count' in status
        assert 'result' in status

    result = JobRunner(job, callback=check_status).run()

    assert result['type'] == expected_status
    assert result['count']['total'] == 2
    assert result['count']['done'] == expected_done
    assert result['count']['errors'] == expected_errors
