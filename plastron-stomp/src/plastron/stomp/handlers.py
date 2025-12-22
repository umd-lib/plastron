import json
import logging
import traceback

from stomp.exception import StompException

from plastron.messaging.broker import Destination
from plastron.messaging.messages import PlastronErrorMessage, PlastronCommandMessage

logger = logging.getLogger(__name__)


class AsynchronousResponseHandler:
    def __init__(self, listener, message: PlastronCommandMessage):
        self.listener = listener
        self.message = message
        self.reply_queue: Destination = listener.broker['JOB_STATUS']

    def __call__(self, future):
        response = self.get_response(future)

        # save a copy of the response message in the outbox
        job_id = response.job_id
        self.listener.outbox.add(job_id, response)

        # remove the message from the inbox now that processing has completed
        self.listener.inbox.remove(self.message.id)

        # send the job completed message
        try:
            self.reply_queue.send(response)
        except StompException as e:
            logger.error(f'Unable to send response message to {self.reply_queue}: {e}')
            # keep the message in the outbox by just returning here;
            # the next time the stomp daemon starts, it will attempt to redeliver
            return

        logger.debug(f'Response message sent to {self.reply_queue} with headers: {response.headers}')

        # remove the message from the outbox now that sending has completed
        self.listener.outbox.remove(job_id)

    def get_response(self, future):
        if e := future.exception():
            # if the command raised an exception, log it and return
            # an error response message
            traceback.print_exc()
            logger.error(f"Job {self.message.job_id} failed: {e}")
            return PlastronErrorMessage(
                job_id=self.message.job_id,
                error=str(e),
                status_url=self.message.status_url,
                body=json.dumps({
                    'state': f'{self.message.command}_error',
                    # TODO: get the actual progress number? if possible?
                    'progress': 0,
                })
            )
        else:
            # assume no errors, return the response
            return future.result()
