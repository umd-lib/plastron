import logging
import traceback

from plastron.messaging.broker import Destination
from plastron.messaging.messages import PlastronMessage, PlastronErrorMessage

logger = logging.getLogger(__name__)


class AsynchronousResponseHandler:
    def __init__(self, listener, message: PlastronMessage):
        self.listener = listener
        self.message = message
        self.reply_queue: Destination = listener.broker['JOB_STATUS']

    def __call__(self, future):
        e = future.exception()
        if e:
            traceback.print_exc()
            logger.error(f"Job {self.message.job_id} failed: {e}")
            response = PlastronErrorMessage(job_id=self.message.job_id, error=str(e))
        else:
            # assume no errors, return the response
            response = future.result()

        # save a copy of the response message in the outbox
        job_id = response.job_id
        self.listener.outbox.add(job_id, response)

        # remove the message from the inbox now that processing has completed
        self.listener.inbox.remove(self.message.id)

        # send the job completed message
        self.reply_queue.send(response)
        logger.debug(f'Response message sent to {self.reply_queue} with headers: {response.headers}')

        # remove the message from the outbox now that sending has completed
        self.listener.outbox.remove(job_id)
