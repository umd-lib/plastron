import importlib.metadata
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Generator, Iterator

from stomp.listener import ConnectionListener

from plastron.context import PlastronContext
from plastron.messaging.broker import Destination
from plastron.messaging.messages import MessageBox, PlastronCommandMessage, PlastronMessage, PlastronResponseMessage
from plastron.stomp.commands import get_command_module, get_module_name
from plastron.stomp.handlers import AsynchronousResponseHandler
from plastron.stomp.inbox_watcher import InboxWatcher

logger = logging.getLogger(__name__)
version = importlib.metadata.version('plastron-stomp')


class CommandListener(ConnectionListener):
    def __init__(self, context: PlastronContext, after_connected: Callable = None, after_disconnected: Callable = None):
        self.context = context
        self.broker = context.broker
        self.inbox = MessageBox(os.path.join(self.broker.message_store_dir, 'inbox'), PlastronCommandMessage)
        self.outbox = MessageBox(os.path.join(self.broker.message_store_dir, 'outbox'), PlastronMessage)
        self.executor = ThreadPoolExecutor(thread_name_prefix=__name__)
        self.inbox_watcher = None
        self.processor = MessageProcessor(context)
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

        self.inbox_watcher = InboxWatcher(self, self.inbox)
        self.inbox_watcher.start()

        if self.after_connected:
            self.after_connected()

    def on_message(self, frame):
        headers = frame.headers
        body = frame.body
        logger.debug(f'Received message on {headers["destination"]} with headers: {headers}')
        if headers['destination'] == self.broker['JOBS'].name:
            # save the message in the inbox until we can process it
            # Note: Processing will occur via the InboxWatcher, which will
            # respond to the inbox placing a file in the inbox message directory
            # containing the message
            message = PlastronCommandMessage(headers=headers, body=body)
            self.inbox.add(message.id, message)
            self.broker.ack(message.id, 'plastron')

    def process_message(self, message, response_handler):
        # send to a message processor thread
        self.executor.submit(self.processor, message, self.broker['JOB_STATUS']).add_done_callback(response_handler)

    def on_disconnected(self):
        logger.warning('Disconnected from the STOMP message broker')
        if self.inbox_watcher:
            self.inbox_watcher.stop()
        if self.after_disconnected:
            self.after_disconnected()


# type alias for command functions that take a context and a message, and return a generator that yields
# status updates in the form of dictionaries, and returns a final state, also as a dictionary
STOMPCommandFunction = Callable[[PlastronContext, PlastronMessage], Generator[dict[str, Any], None, dict[str, Any]]]


def get_command(command_name: str) -> STOMPCommandFunction:
    module_name = get_module_name(command_name)
    module = get_command_module(command_name)
    command = getattr(module, module_name, None)
    if command is None:
        raise RuntimeError(f'Command function "{module_name}" not found in {module.__name__}')
    return command


class MessageProcessor:
    def __init__(self, context: PlastronContext):
        self.context = context
        # cache for command instances
        self.commands = {}
        self.result = None

    def __call__(self, message: PlastronCommandMessage, progress_topic: Destination):
        if message.job_id is None:
            raise RuntimeError('Expecting a PlastronJobId header')

        logger.info(f'Received message to initiate job {message.job_id}')

        if message.status_url is not None:
            logger.info(f'Callback status notifications will be sent to {message.status_url}')

        # determine which command to load to process the message
        command = get_command(message.command)

        delegated_user = message.args.get('on-behalf-of')
        if delegated_user is not None:
            logger.info(f'Running repository operations on behalf of {delegated_user}')

        # run the command, and send a progress message over STOMP every time it yields
        # the _run() delegating generator captures the final status in self.result
        with self.context.repo_configuration(
            delegated_user=delegated_user,
            ua_string=f'plastron/{version}',
        ) as run_context:
            for status in self._run(command(run_context, message)):
                progress_topic.send(
                    PlastronResponseMessage(
                        job_id=message.job_id,
                        status_url=message.status_url,
                        body=status,
                    ))

        logger.info(f'Job {message.job_id} complete')

        # default message state is "Done"
        return message.response(state=self.result.get('type', 'Done'), body=self.result)

    def _run(self, command: Generator[dict, None, dict]) -> Iterator[dict[str, Any]]:
        # delegating generator; each progress step is passed to the calling
        # method, and the return value from the command is stored as the result
        self.result = yield from command
