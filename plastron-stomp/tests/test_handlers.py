from concurrent.futures import Future
from typing import cast, Type
from unittest import TestCase
from unittest.mock import Mock

import pytest

from plastron.messaging.broker import Destination
from plastron.messaging.messages import MessageBox, PlastronCommandMessage, PlastronErrorMessage, PlastronMessage, \
    Message
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


def test_asynchronous_response_handler_successful_call_removes_inbox_and_outbox_entries(mock_listener):
    # Set up mocks
    job_id = 'test_asynchronous_response_handler_successful_call-123'

    # This future represents a successful call
    future_response = PlastronMessage(headers={'PlastronJobId': job_id}, body="Success!")
    future_mock_attrs = {'exception.return_value': None, 'result.return_value': future_response}
    future = Mock(Future, **future_mock_attrs)

    incoming_message = Mock(PlastronCommandMessage, id='incoming_test_message', job_id=job_id)

    # Handle the incoming message
    handler = AsynchronousResponseHandler(mock_listener, incoming_message)
    handler(future)

    # Verify outbox/inbox handling and message sending
    mock_listener.outbox.add.assert_called_once_with(job_id, future_response)
    mock_listener.inbox.remove.assert_called_once_with(incoming_message.id)
    mock_listener.broker['JOB_STATUS'].send.assert_called_once_with(future_response)
    mock_listener.outbox.remove.assert_called_once_with(job_id)


def test_asynchronous_response_handler_call_with_exception_removes_inbox_and_outbox_entries(mock_listener):
    # Set up mocks
    job_id = 'test_asynchronous_response_handler_call_with_exception-123'

    # This future throw an exception, representing a failed call
    future_response = PlastronErrorMessage(headers={'PlastronJobId': job_id})
    exception_message = 'An error occurred'
    future_mock_attrs = {'exception.return_value': Exception(exception_message), 'result.return_value': future_response}
    future = Mock(Future, **future_mock_attrs)

    incoming_message = Mock(PlastronCommandMessage, id='incoming_test_message', job_id=job_id)

    # Handle the incoming message
    handler = AsynchronousResponseHandler(mock_listener, incoming_message)
    handler(future)

    # Verify outbox/inbox handling and message sending
    mock_listener.outbox.add.assert_called_once()
    mock_listener.inbox.remove.assert_called_once_with(incoming_message.id)

    mock_listener.broker['JOB_STATUS'].send.assert_called_once()
    expected_msg_headers = {'PlastronJobId': job_id, 'PlastronJobError': exception_message, 'persistent': 'true'}
    assert_sent_message(mock_listener.broker['JOB_STATUS'], PlastronErrorMessage, expected_msg_headers, '')

    mock_listener.outbox.remove.assert_called_once_with(job_id)


def assert_sent_message(queue: Destination, message_class: Type[Message],
                        expected_message_headers: dict[str, str], expected_body: str):
    status_queue_send_args = cast(Mock, queue).send.call_args[0]  # using cast, so mypy doesn't complain
    sent_message = status_queue_send_args[0]
    assert isinstance(sent_message, message_class)
    TestCase().assertDictEqual(sent_message.headers, expected_message_headers)
    assert sent_message.body == expected_body
