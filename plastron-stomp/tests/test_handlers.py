from concurrent.futures import Future
from typing import cast
from unittest.mock import Mock, MagicMock

import pytest
from stomp.exception import StompException

from plastron.messaging.broker import Destination
from plastron.messaging.messages import MessageBox, PlastronCommandMessage, PlastronErrorMessage, PlastronMessage
from plastron.stomp.handlers import AsynchronousResponseHandler
from plastron.stomp.listeners import CommandListener


@pytest.fixture
def mock_listener():
    return Mock(
        spec=CommandListener,
        inbox=Mock(MessageBox),
        outbox=Mock(MessageBox),
        broker={'JOB_STATUS': Mock(Destination)},
    )


@pytest.fixture
def incoming_message():
    return PlastronCommandMessage(
        job_id='test_message',
        command='test',
        status_url='http://example.com/status',
    )


@pytest.fixture
def mock_future():
    def _mock_future(result: PlastronMessage):
        # This future represents a successful call
        future_mock_attrs = {'exception.return_value': None, 'result.return_value': result}
        return Mock(Future, **future_mock_attrs)

    return _mock_future


@pytest.fixture
def mock_future_failure():
    def _mock_future(exception: Exception):
        future = MagicMock(spec=Future)
        future.exception.return_value = exception
        return future
    return _mock_future


def test_asynchronous_response_handler_successful_call_removes_inbox_and_outbox_entries(mock_listener, mock_future, incoming_message):
    job_id = incoming_message.job_id
    result = PlastronMessage(headers={'PlastronJobId': job_id}, body="Success!")
    future = mock_future(result)

    # Handle the incoming message
    handler = AsynchronousResponseHandler(mock_listener, incoming_message)
    handler(future)

    # Verify outbox/inbox handling and message sending
    mock_listener.outbox.add.assert_called_once_with(job_id, result)
    mock_listener.inbox.remove.assert_called_once_with(incoming_message.id)
    mock_listener.broker['JOB_STATUS'].send.assert_called_once_with(result)
    mock_listener.outbox.remove.assert_called_once_with(job_id)


def test_asynchronous_response_handler_call_with_exception_removes_inbox_and_outbox_entries(mock_listener, mock_future_failure, incoming_message):
    job_id = incoming_message.job_id
    exception_message = 'An error occurred'

    # Handle the incoming message
    handler = AsynchronousResponseHandler(mock_listener, incoming_message)
    handler(mock_future_failure(RuntimeError(exception_message)))

    # Verify outbox/inbox handling and message sending
    mock_listener.outbox.add.assert_called_once()
    mock_listener.inbox.remove.assert_called_once_with(incoming_message.id)

    mock_listener.broker['JOB_STATUS'].send.assert_called_once()
    expected_message_headers = {
        'PlastronJobId': job_id,
        'PlastronJobError': exception_message,
        'PlastronStatusURL': 'http://example.com/status',
        'persistent': 'true',
    }
    queue = mock_listener.broker['JOB_STATUS']
    message_class = PlastronErrorMessage
    expected_body = '{"state": "test_error", "progress": 0}'
    status_queue_send_args = cast(Mock, queue).send.call_args[0]  # using cast, so mypy doesn't complain
    sent_message = status_queue_send_args[0]
    assert isinstance(sent_message, message_class)
    assert sent_message.headers == expected_message_headers
    assert sent_message.body == expected_body
    mock_listener.outbox.remove.assert_called_once_with(job_id)


def test_handler_preserves_response_on_exception(mock_listener, mock_future, incoming_message):
    # Set up mocks
    job_id = incoming_message.job_id
    mock_destination = MagicMock(spec=Destination)
    mock_destination.send.side_effect = StompException
    mock_listener.broker['JOB_STATUS'] = mock_destination

    result = PlastronMessage(headers={'PlastronJobId': job_id}, body="Success!")
    future = mock_future(result)

    # Handle the incoming message
    handler = AsynchronousResponseHandler(mock_listener, incoming_message)
    handler(future)

    # Verify outbox/inbox handling and message sending
    mock_listener.outbox.add.assert_called_once_with(job_id, result)
    mock_listener.inbox.remove.assert_called_once_with(incoming_message.id)
    mock_listener.broker['JOB_STATUS'].send.assert_called_once_with(result)
    mock_listener.outbox.remove.assert_not_called()


def test_handler_exception_creates_error_message(mock_listener, mock_future_failure, incoming_message):
    exception_message = 'An error occurred'
    handler = AsynchronousResponseHandler(mock_listener, incoming_message)
    response = handler.get_response(mock_future_failure(RuntimeError(exception_message)))
    assert isinstance(response, PlastronErrorMessage)
    assert response.job_id == incoming_message.job_id
    assert response.status_url == incoming_message.status_url
    assert response.error == 'An error occurred'
