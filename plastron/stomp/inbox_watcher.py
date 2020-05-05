import os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent

logger = logging.getLogger(__name__)


class InboxEventHandler(FileSystemEventHandler):
    """Triggers message processing when a file is added or modified in the
       inbox directory."""
    def __init__(self, command_listener, message_box):
        self.command_listener = command_listener
        self.message_box = message_box

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent):
            logger.info(f"Triggering inbox processing due to {event}")
            message = self.message_box.message_class.read(event.src_path)
            self.command_listener.process_message(message)

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent):
            logger.info(f"Triggering inbox processing due to {event}")
            message = self.message_box.message_class.read(event.src_path)
            self.command_listener.process_message(message)


class InboxWatcher:
    """
    Watches for changes to the inbox directory, in order to trigger message
    processing via InboxEventHandler
    """
    def __init__(self, command_listener, message_box):
        """Constructs the watchdog Observer"""
        self.observer = Observer()
        self.observer.schedule(InboxEventHandler(command_listener, message_box), message_box.dir, recursive=True)

    def start(self):
        """Start the watcher"""
        logger.debug(f"Starting InboxWatcher")
        self.observer.start()

    def stop(self):
        """Stop the watcher"""
        logger.debug(f"Stopping InboxWatcher")
        self.observer.unschedule_all()
        self.observer.stop()
