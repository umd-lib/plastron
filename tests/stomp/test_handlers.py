from unittest.mock import Mock, patch
from concurrent.futures import Future

from plastron.stomp import Destination
from plastron.stomp.handlers import AsynchronousResponseHandler
from plastron.stomp.listeners import CommandListener
from plastron.stomp.messages import Message, MessageBox, PlastronCommandMessage


def test_asynchronous_response_handler_successful_call_removes_inbox_and_outbox_entries():
    listener = Mock(CommandListener, inbox=Mock(MessageBox), outbox=Mock(MessageBox), status_queue=Mock(Destination))

    job_id = 'test_asynchronous_response_handler_successful_call-123'
    incoming_message = Mock(PlastronCommandMessage, id='incoming_test_message', job_id=job_id)

    future_response = Message(headers={'PlastronJobId': job_id}, body="Success!")
    future_mock_attrs = {'exception.return_value': None, 'result.return_value': future_response}
    future = Mock(Future, **future_mock_attrs)

    handler = AsynchronousResponseHandler(listener, incoming_message)
    handler(future)

    # Verify outbox/inbox handling and message sending
    listener.outbox.add.assert_called_once_with(job_id, future_response)
    listener.inbox.remove.assert_called_once_with(incoming_message.id)
    listener.status_queue.send.assert_called_once_with(headers=future_response.headers, body=future_response.body)
    listener.outbox.remove.assert_called_once_with(job_id)


def test_asynchronous_response_handler_call_with_exception_removes_inbox_and_outbox_entries():
    job_id = 'test_asynchronous_response_handler_call_with_exception-123'
    incoming_message = Mock(PlastronCommandMessage, id='incoming_test_message', job_id=job_id)

    future_response = Message(headers={'PlastronJobId': job_id})
    exception_message = 'An error occurred'
    future_mock_attrs = {'exception.return_value': Exception(exception_message), 'result.return_value': future_response}
    future = Mock(Future, **future_mock_attrs)

    listener = Mock(CommandListener, inbox=Mock(MessageBox), outbox=Mock(MessageBox), status_queue=Mock(Destination))

    handler = AsynchronousResponseHandler(listener, incoming_message)
    handler(future)

    # Verify outbox/inbox handling and message sending
    listener.outbox.add.called_once
    listener.inbox.remove.assert_called_once_with(incoming_message.id)
    expected_headers = {'PlastronJobId': job_id, 'PlastronJobError': exception_message, 'persistent': 'true'}
    listener.status_queue.send.assert_called_once_with(headers=expected_headers, body='')
    listener.outbox.remove.assert_called_once_with(job_id)
