import logging

from plastron.stomp.messages import Message
from plastron.stomp import Destination

logger = logging.getLogger(__name__)


class AsynchronousResponseHandler:
    def __init__(self, listener, message):
        self.listener = listener
        self.message = message

    def __call__(self, future):
        e = future.exception()
        if e:
            logger.error(f"Job {self.message.job_id} failed: {e}")
            response = self.create_exception_response(self.message.job_id, e)
        else:
            # assume no errors, return the response
            response = future.result()

        # save a copy of the response message in the outbox
        job_id = response.headers['PlastronJobId']
        self.listener.outbox.add(job_id, response)

        # remove the message from the inbox now that processing has completed
        self.listener.inbox.remove(self.message.id)

        # send the response message
        self.listener.status_queue.send(headers=response.headers, body=response.body)
        logger.debug(f'Response message sent to {self.listener.status_queue} with headers: {response.headers}')

        # remove the message from the outbox now that sending has completed
        self.listener.outbox.remove(job_id)

    def create_exception_response(self, job_id, e):
        """Returns the Message to send when an exception has occurred."""
        return Message(headers={
            'PlastronJobId': job_id,
            'PlastronJobError': str(e),
            'persistent': 'true'
        })


class SynchronousResponseHandler:
    def __init__(self, listener, message):
        self.listener = listener
        self.message = message
        self.reply_to = Destination(self.listener.broker, message.headers['reply-to'])

    def __call__(self, future):
        e = future.exception()
        if e:
            logger.error(f"Job {self.message.job_id} failed: {e}")
            self.reply_to.send(
                headers={
                    'PlastronJobId': self.message.job_id,
                    'PlastronJobError': str(e),
                    'persistent': 'true'
                }
            )
        else:
            # assume no errors, return the response
            response = future.result()
            # send to the specified "reply to" queue
            self.reply_to.send(
                body=response.body,
                headers=response.headers
            )
            logger.debug(f'Response message sent to {self.reply_to} with headers: {response.headers}')
