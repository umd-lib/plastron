import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from plastron import version
from plastron.exceptions import FailureException, RESTAPIException
from plastron.http import Repository
from plastron.stomp import MessageBox, PlastronCommandMessage, PlastronMessage, Message
from stomp.listener import ConnectionListener
from plastron.stomp.inbox_watcher import InboxWatcher

logger = logging.getLogger(__name__)


class CommandListener(ConnectionListener):
    def __init__(self, broker, repo_config):
        self.broker = broker
        self.repo_config = repo_config
        self.queue = self.broker.destinations['JOBS']
        self.completed_queue = self.broker.destinations['COMPLETED_JOBS']
        self.status_topic = self.broker.destinations['JOB_STATUS']
        self.inbox = MessageBox(os.path.join(self.broker.message_store_dir, 'inbox'))
        self.outbox = MessageBox(os.path.join(self.broker.message_store_dir, 'outbox'))
        self.executor = ThreadPoolExecutor(thread_name_prefix=__name__)
        self.public_uri_template = self.broker.public_uri_template
        self.inbox_watcher = None

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
            self.process_message(message)

        # then subscribe to the queue to receive incoming messages
        self.broker.connection.subscribe(destination=self.queue, id='plastron')
        logger.info(f"Subscribed to {self.queue}")

        self.inbox_watcher = InboxWatcher(self, self.inbox)
        self.inbox_watcher.start()

    def on_message(self, headers, body):
        # Note: Processing will occur via the InboxWatcher, which will
        # respond to the inbox placing a file in the inbox message directory
        # containing the message

        if headers['destination'] == self.queue:
            logger.debug(f'Received message on {self.queue} with headers: {headers}')

            # save the message in the inbox until we can process it
            message = PlastronCommandMessage(headers=headers, body=body)
            self.inbox.add(message.id, message)

    def process_message(self, message):
        # determine which command to load to process the message
        command_module = import_module('plastron.commands.' + message.command)
        # TODO: cache the command modules
        # TODO: check that the command module supports message processing

        repo = Repository(
            config=self.repo_config,
            ua_string=f'plastron/{version}',
            on_behalf_of=message.args.get('on-behalf-of')
        )

        # define the processor for this message
        def process():
            try:
                if message.job_id is None:
                    raise FailureException('Expecting a PlastronJobId header')

                logger.info(f'Received message to initiate job {message.job_id}')
                if repo.delegated_user is not None:
                    logger.info(f'Running repository operations on behalf of {repo.delegated_user}')

                args = command_module.parse_message(message)
                command = command_module.Command()

                for status in command.execute(repo, args):
                    self.broker.connection.send(
                        self.status_topic,
                        headers={
                            'PlastronJobId': message.job_id
                        },
                        body=json.dumps(status)
                    )

                logger.info(f'Job {message.job_id} complete')

                return Message(
                    headers={
                        'PlastronJobId': message.job_id,
                        'PlastronJobStatus': 'Done',
                        'persistent': 'true'
                    },
                    body=json.dumps(command.result)
                )

            except (FailureException, RESTAPIException) as e:
                logger.error(f"Job {message.job_id} failed: {e}")
                return Message(
                    headers={
                        'PlastronJobId': message.job_id,
                        'PlastronJobStatus': 'Error',
                        'PlastronJobError': str(e),
                        'persistent': 'true'
                    }
                )

        # process message
        self.executor.submit(process).add_done_callback(self.get_response_handler(message.id))

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
            logger.debug(f'Response message sent to {self.completed_queue} with headers: {response.headers}')

            # remove the message from the outbox now that sending has completed
            self.outbox.remove(job_id)

        return response_handler

    def on_disconnected(self):
        if self.inbox_watcher:
            self.inbox_watcher.stop()


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
