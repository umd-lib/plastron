import logging
import os

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from plastron.stomp import MessageBox, PlastronCommandMessage, PlastronMessage
from stomp.listener import ConnectionListener

logger = logging.getLogger(__name__)


class CommandListener(ConnectionListener):
    def __init__(self, broker, repository):
        self.broker = broker
        self.repository = repository
        self.queue = self.broker.destinations['JOBS']
        self.completed_queue = self.broker.destinations['COMPLETED_JOBS']
        self.inbox = MessageBox(os.path.join(self.broker.message_store_dir, 'inbox'))
        self.outbox = MessageBox(os.path.join(self.broker.message_store_dir, 'outbox'))
        self.executor = ThreadPoolExecutor(thread_name_prefix=__name__)
        self.public_uri_template = self.broker.public_uri_template

    def on_connected(self, headers, body):
        # first attempt to send anything in the outbox
        for message in self.outbox(PlastronMessage):
            logger.info(f"Found response message for job {message.job_id} in outbox")
            # send the job completed message
            self.broker.connection.send(self.completed_queue, headers=message.headers, body=message.body)
            logger.info(f'Sent response message for job {message.job_id}')
            # remove the message from the outbox now that sending has completed
            self.outbox.remove(message.job_id)

        # then process anything in the inbox
        for message in self.inbox(PlastronCommandMessage):
            self.dispatch(message)

        # then subscribe to the queue to receive incoming messages
        self.broker.connection.subscribe(destination=self.queue, id='plastron')
        logger.info(f"Subscribed to {self.queue}")

    def dispatch(self, message):
        # determine which command to load to process it
        command_module = import_module('plastron.commands.' + message.command)
        # TODO: cache the command modules
        # TODO: check that process_message exists in the command module
        command_module.process_message(self, message)

    def on_message(self, headers, body):
        if headers['destination'] == self.queue:
            logger.debug(f'Received message on {self.queue} with headers: {headers}')

            # save the message in the inbox until we can process it
            message = PlastronCommandMessage(headers=headers, body=body)
            self.inbox.add(message.id, message)

            # and then process the message
            self.dispatch(message)

    def get_response_handler(self, message_id):
        # define the response handler for this message
        def response_handler(future):
            response = future.result()

            # save a copy of the response message in the outbox
            job_id = response.headers['PlastronJobId']
            self.outbox.add(job_id, response)

            # remove the message from the inbox now that processing has completed
            self.inbox.remove(message_id)

            # send the job completed message
            self.broker.connection.send(self.completed_queue, headers=response.headers, body=response.body)

            # remove the message from the outbox now that sending has completed
            self.outbox.remove(job_id)

        return response_handler


class ReconnectListener(ConnectionListener):
    def __init__(self, broker):
        self.broker = broker

    def on_connecting(self, host_and_port):
        logger.info(f'Connecting to STOMP message broker at {":".join(host_and_port)}')

    def on_connected(self, headers, body):
        logger.info('Connected to STOMP message broker')

    def on_disconnected(self):
        logger.warning('Disconnected from the STOMP message broker')
        self.broker.connect()
