import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from plastron import version
from plastron.exceptions import FailureException
from plastron.http import Repository
from plastron.stomp import Destination
from plastron.stomp.handlers import AsynchronousResponseHandler, SynchronousResponseHandler
from plastron.stomp.inbox_watcher import InboxWatcher
from plastron.stomp.messages import MessageBox, PlastronCommandMessage, PlastronMessage, Message
from stomp.listener import ConnectionListener


logger = logging.getLogger(__name__)


class CommandListener(ConnectionListener):
    def __init__(self, broker, repo_config, command_config):
        self.broker = broker
        self.repo_config = repo_config
        self.queue = self.broker.destinations['JOBS']
        self.status_queue = Destination(self.broker, self.broker.destinations['JOB_STATUS'])
        self.progress_topic = Destination(self.broker, self.broker.destinations['JOB_PROGRESS'])
        self.synchronous_queue = self.broker.destinations['SYNCHRONOUS_JOBS']
        self.inbox = MessageBox(os.path.join(self.broker.message_store_dir, 'inbox'))
        self.outbox = MessageBox(os.path.join(self.broker.message_store_dir, 'outbox'))
        self.executor = ThreadPoolExecutor(thread_name_prefix=__name__)
        self.public_uri_template = self.broker.public_uri_template
        self.inbox_watcher = None
        self.command_config = command_config
        self.processor = MessageProcessor(command_config, repo_config)

    def on_connected(self, headers, body):
        # first attempt to send anything in the outbox
        for message in self.outbox(PlastronMessage):
            logger.info(f"Found response message for job {message.job_id} in outbox")
            # send the job completed message
            self.status_queue.send(headers=message.headers, body=message.body)
            logger.info(f'Sent response message for job {message.job_id}')
            # remove the message from the outbox now that sending has completed
            self.outbox.remove(message.job_id)

        # then process anything in the inbox
        for message in self.inbox(PlastronCommandMessage):
            self.process_message(message, AsynchronousResponseHandler(self, message))

        # then subscribe to the queue to receive incoming messages
        self.broker.connection.subscribe(
            destination=self.queue,
            id='plastron',
            ack='client-individual'
        )
        logger.info(f"Subscribed to {self.queue}")

        # Subscribe for synchronous jobs
        self.broker.connection.subscribe(
            destination=self.synchronous_queue,
            id='plastron-synchronous',
            ack='client-individual'
        )
        logger.info(f"Subscribed to {self.synchronous_queue} for synchronous jobs")

        self.inbox_watcher = InboxWatcher(self, self.inbox)
        self.inbox_watcher.start()

    def on_message(self, headers, body):
        if headers['destination'] == self.synchronous_queue:
            logger.debug(f'Received synchronous job message on {self.synchronous_queue} with headers: {headers}')
            message = PlastronCommandMessage(headers=headers, body=body)
            self.process_message(message, SynchronousResponseHandler(self, message))
            self.broker.connection.ack(message.id, 'plastron-synchronous')

        elif headers['destination'] == self.queue:
            logger.debug(f'Received message on {self.queue} with headers: {headers}')

            # save the message in the inbox until we can process it
            # Note: Processing will occur via the InboxWatcher, which will
            # respond to the inbox placing a file in the inbox message directory
            # containing the message
            message = PlastronCommandMessage(headers=headers, body=body)
            self.inbox.add(message.id, message)
            self.broker.connection.ack(message.id, 'plastron')

    def process_message(self, message, response_handler):
        # send to a message processor thread
        self.executor.submit(self.processor, message, self.progress_topic).add_done_callback(response_handler)

    def on_disconnected(self):
        if self.inbox_watcher:
            self.inbox_watcher.stop()


class MessageProcessor:
    def __init__(self, command_config, repo_config):
        self.command_config = command_config
        self.repo_config = repo_config
        # cache for command instances
        self.commands = {}

    def get_command(self, command_name):
        if command_name not in self.commands:
            try:
                command_module = import_module('plastron.commands.' + command_name)
            except ModuleNotFoundError as e:
                raise FailureException(f'Unable to load a command with the name {command_name}') from e
            command_class = getattr(command_module, 'Command')
            if command_class is None:
                raise FailureException(f'Command class not found in module {command_module}')
            if getattr(command_class, 'parse_message') is None:
                raise FailureException(f'Command class in {command_module} does not support message processing')

            # get the configuration options for this command
            config = self.command_config.get(command_name.upper(), {})

            # cache an instance of this command
            self.commands[command_name] = command_class(config)

        return self.commands[command_name]

    def __call__(self, message, progress_topic):
        # determine which command to load to process the message
        command = self.get_command(message.command)

        if message.job_id is None:
            raise FailureException('Expecting a PlastronJobId header')

        logger.info(f'Received message to initiate job {message.job_id}')

        args = command.parse_message(message)

        cmd_repo_config = command.repo_config(self.repo_config, args)

        repo = Repository(
            config=cmd_repo_config,
            ua_string=f'plastron/{version}',
            on_behalf_of=message.args.get('on-behalf-of')
        )

        if repo.delegated_user is not None:
            logger.info(f'Running repository operations on behalf of {repo.delegated_user}')

        for status in (command.execute(repo, args) or []):
            progress_topic.send(
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


class ReconnectListener(ConnectionListener):
    def __init__(self, broker):
        self.broker = broker

    def on_connecting(self, host_and_port):
        logger.info(f'Connecting to STOMP message broker at {":".join(host_and_port)}')

    def on_connected(self, headers, body):
        logger.info('Connected to STOMP message broker')

    def on_heartbeat_timeout(self):
        logger.warning('Missed a heartbeat, assuming disconnection from the STOMP message broker')
        self.broker.connect()

    def on_disconnected(self):
        logger.warning('Disconnected from the STOMP message broker')
        self.broker.connect()
