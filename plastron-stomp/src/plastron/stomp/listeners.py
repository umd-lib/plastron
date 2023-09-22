import importlib.metadata
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Callable

from stomp.listener import ConnectionListener

from plastron.client import RepositoryStructure
from plastron.repo import Repository
from plastron.stomp.broker import Broker, Destination
from plastron.stomp.commands import get_command_class
from plastron.stomp.handlers import AsynchronousResponseHandler, SynchronousResponseHandler
from plastron.stomp.inbox_watcher import InboxWatcher
from plastron.stomp.messages import MessageBox, PlastronCommandMessage, PlastronMessage

logger = logging.getLogger(__name__)
version = importlib.metadata.version('plastron-stomp')


class CommandListener(ConnectionListener):
    def __init__(
            self,
            broker: Broker,
            repo_config: Dict[str, Any],
            command_config: Dict[str, Any] = None,
            after_connected: Callable = None,
            after_disconnected: Callable = None,
    ):
        self.broker = broker
        self.repo_config = repo_config
        self.inbox = MessageBox(os.path.join(self.broker.message_store_dir, 'inbox'), PlastronCommandMessage)
        self.outbox = MessageBox(os.path.join(self.broker.message_store_dir, 'outbox'), PlastronMessage)
        self.executor = ThreadPoolExecutor(thread_name_prefix=__name__)
        self.public_uri_template = self.broker.public_uri_template
        self.inbox_watcher = None
        self.command_config = command_config or {}
        self.processor = MessageProcessor(self.command_config, self.repo_config)
        self.after_connected = after_connected
        self.after_disconnected = after_disconnected

    def on_connecting(self, host_and_port):
        logger.info(f'Connecting to STOMP message broker {self.broker}')

    def on_connected(self, frame):
        logger.info(f'Connected to STOMP message broker {self.broker}')

        # first attempt to send anything in the outbox
        for message in self.outbox:
            logger.info(f"Found response message for job {message.job_id} in outbox")
            # send the job completed message
            self.broker['JOB_STATUS'].send(message)
            logger.info(f'Sent response message for job {message.job_id}')
            # remove the message from the outbox now that sending has completed
            self.outbox.remove(message.job_id)

        # then process anything in the inbox
        for message in self.inbox:
            self.process_message(message, AsynchronousResponseHandler(self, message))

        # subscribe to receive asynchronous jobs
        self.broker['JOBS'].subscribe(id='plastron', ack='client-individual')
        # subscribe to receive synchronous jobs
        self.broker['SYNCHRONOUS_JOBS'].subscribe(id='plastron-synchronous', ack='client-individual')

        self.inbox_watcher = InboxWatcher(self, self.inbox)
        self.inbox_watcher.start()

        if self.after_connected:
            self.after_connected()

    def on_message(self, frame):
        headers = frame.headers
        body = frame.body
        logger.debug(f'Received message on {headers["destination"]} with headers: {headers}')
        if headers['destination'] == self.broker['SYNCHRONOUS_JOBS'].name:
            message = PlastronCommandMessage(headers=headers, body=body)
            self.process_message(message, SynchronousResponseHandler(self, message))
            self.broker.ack(message.id, 'plastron-synchronous')

        elif headers['destination'] == self.broker['JOBS'].name:
            # save the message in the inbox until we can process it
            # Note: Processing will occur via the InboxWatcher, which will
            # respond to the inbox placing a file in the inbox message directory
            # containing the message
            message = PlastronCommandMessage(headers=headers, body=body)
            self.inbox.add(message.id, message)
            self.broker.ack(message.id, 'plastron')

    def process_message(self, message, response_handler):
        # send to a message processor thread
        self.executor.submit(self.processor, message, self.broker['JOB_PROGRESS']).add_done_callback(response_handler)

    def on_disconnected(self):
        logger.warning('Disconnected from the STOMP message broker')
        if self.inbox_watcher:
            self.inbox_watcher.stop()
        if self.after_disconnected:
            self.after_disconnected()


class MessageProcessor:
    def __init__(self, command_config, repo_config):
        self.command_config = command_config
        self.repo_config = repo_config
        # cache for command instances
        self.commands = {}

    def get_command(self, command_name: str):
        if command_name not in self.commands:
            # get the configuration options for this command
            config = self.command_config.get(command_name.upper(), {})
            command_class = get_command_class(command_name)
            # cache an instance of this command
            self.commands[command_name] = command_class(config)

        return self.commands[command_name]

    def configure_repo(self, message: PlastronCommandMessage) -> Repository:
        repo = Repository.from_config(self.repo_config)
        repo.ua_string = f'plastron/{version}',
        repo.delegated_user = message.args.get('on-behalf-of'),
        if 'structure' in message.args:
            repo.client.structure = RepositoryStructure[message.args['structure'].upper()]
        if 'relpath' in message.args:
            repo.endpoint.relpath = message.args['relpath']
        if repo.client.delegated_user is not None:
            logger.info(f'Running repository operations on behalf of {repo.client.delegated_user}')
        return repo

    def __call__(self, message: PlastronCommandMessage, progress_topic: Destination):
        if message.job_id is None:
            raise RuntimeError('Expecting a PlastronJobId header')

        logger.info(f'Received message to initiate job {message.job_id}')
        repo = self.configure_repo(message)

        # determine which command to load to process the message
        command = self.get_command(message.command)

        for status in command(repo, message):
            progress_topic.send(PlastronMessage(job_id=message.job_id, body=status))

        logger.info(f'Job {message.job_id} complete')

        # default message state is "Done"
        return message.response(state=command.result.get('type', 'Done'), body=command.result)
