import importlib.metadata
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from stomp.listener import ConnectionListener

from plastron.client import Repository, Client, get_authenticator
from plastron.commands import get_command_class
from plastron.exceptions import FailureException
from plastron.stomp import Destination
from plastron.stomp.handlers import AsynchronousResponseHandler, SynchronousResponseHandler
from plastron.stomp.inbox_watcher import InboxWatcher
from plastron.stomp.messages import MessageBox, PlastronCommandMessage, PlastronMessage

logger = logging.getLogger(__name__)
version = importlib.metadata.version('plastron')


class CommandListener(ConnectionListener):
    def __init__(self, thread: 'STOMPDaemon'):
        self.thread = thread
        self.broker = thread.broker
        self.repo_config = thread.config['REPOSITORY']
        self.queue = self.broker.destinations['JOBS']
        self.status_queue = Destination(self.broker, self.broker.destinations['JOB_STATUS'])
        self.progress_topic = Destination(self.broker, self.broker.destinations['JOB_PROGRESS'])
        self.synchronous_queue = self.broker.destinations['SYNCHRONOUS_JOBS']
        self.inbox = MessageBox(os.path.join(self.broker.message_store_dir, 'inbox'), PlastronCommandMessage)
        self.outbox = MessageBox(os.path.join(self.broker.message_store_dir, 'outbox'), PlastronMessage)
        self.executor = ThreadPoolExecutor(thread_name_prefix=__name__)
        self.public_uri_template = self.broker.public_uri_template
        self.inbox_watcher = None
        self.command_config = thread.config['COMMANDS']
        self.processor = MessageProcessor(self.command_config, self.repo_config)

    def on_connecting(self, host_and_port):
        logger.info(f'Connecting to STOMP message broker {self.broker}')

    def on_connected(self, frame):
        logger.info(f'Connected to STOMP message broker {self.broker}')

        # first attempt to send anything in the outbox
        for message in self.outbox:
            logger.info(f"Found response message for job {message.job_id} in outbox")
            # send the job completed message
            self.status_queue.send(message)
            logger.info(f'Sent response message for job {message.job_id}')
            # remove the message from the outbox now that sending has completed
            self.outbox.remove(message.job_id)

        # then process anything in the inbox
        for message in self.inbox:
            self.process_message(message, AsynchronousResponseHandler(self, message))

        # then subscribe to the queue to receive incoming messages
        self.broker.subscribe(
            destination=self.queue,
            id='plastron',
            ack='client-individual'
        )
        logger.info(f"Subscribed to {self.queue}")

        # Subscribe for synchronous jobs
        self.broker.subscribe(
            destination=self.synchronous_queue,
            id='plastron-synchronous',
            ack='client-individual'
        )
        logger.info(f"Subscribed to {self.synchronous_queue} for synchronous jobs")

        self.inbox_watcher = InboxWatcher(self, self.inbox)
        self.inbox_watcher.start()

        self.thread.stopped.clear()
        self.thread.started.set()

    def on_message(self, frame):
        headers = frame.headers
        body = frame.body
        if headers['destination'] == self.synchronous_queue:
            logger.debug(f'Received synchronous job message on {self.synchronous_queue} with headers: {headers}')
            message = PlastronCommandMessage(headers=headers, body=body)
            self.process_message(message, SynchronousResponseHandler(self, message))
            self.broker.ack(message.id, 'plastron-synchronous')

        elif headers['destination'] == self.queue:
            logger.debug(f'Received message on {self.queue} with headers: {headers}')

            # save the message in the inbox until we can process it
            # Note: Processing will occur via the InboxWatcher, which will
            # respond to the inbox placing a file in the inbox message directory
            # containing the message
            message = PlastronCommandMessage(headers=headers, body=body)
            self.inbox.add(message.id, message)
            self.broker.ack(message.id, 'plastron')

    def process_message(self, message, response_handler):
        # send to a message processor thread
        self.executor.submit(self.processor, message, self.progress_topic).add_done_callback(response_handler)

    def on_disconnected(self):
        logger.warning('Disconnected from the STOMP message broker')
        if self.inbox_watcher:
            self.inbox_watcher.stop()
        self.thread.started.clear()
        self.thread.stopped.set()


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
            if getattr(command_class, 'parse_message') is None:
                raise FailureException(f'Command class {command_class} does not support message processing')

            # cache an instance of this command
            self.commands[command_name] = command_class(config)

        return self.commands[command_name]

    def __call__(self, message: PlastronCommandMessage, progress_topic: Destination):
        # determine which command to load to process the message
        command = self.get_command(message.command)

        if message.job_id is None:
            raise FailureException('Expecting a PlastronJobId header')

        logger.info(f'Received message to initiate job {message.job_id}')

        args = command.parse_message(message)

        repo_config = command.repo_config(self.repo_config, args)

        repo = Repository(
            endpoint=repo_config['REST_ENDPOINT'],
            default_path=repo_config.get('RELPATH', '/'),
            external_url=repo_config.get('REPO_EXTERNAL_URL'),
        )
        # TODO: respect the batch mode flag when getting the authenticator
        client = Client(
            repo=repo,
            auth=get_authenticator(repo_config),
            ua_string=f'plastron/{version}',
            on_behalf_of=message.args.get('on-behalf-of'),
        )

        if client.delegated_user is not None:
            logger.info(f'Running repository operations on behalf of {client.delegated_user}')

        for status in (command.execute(client, args) or []):
            progress_topic.send(PlastronMessage(job_id=message.job_id, body=status))

        logger.info(f'Job {message.job_id} complete')

        # default message state is "Done"
        return message.response(state=command.result.get('type', 'Done'), body=command.result)
