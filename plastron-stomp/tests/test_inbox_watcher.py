import os
import tempfile
import time
from contextlib import contextmanager
from unittest.mock import patch

from plastron.stomp.inbox_watcher import InboxWatcher
from plastron.messaging.messages import MessageBox, PlastronMessage


def test_new_file_in_inbox():
    with tempfile.TemporaryDirectory() as inbox_dirname:
        with patch('plastron.stomp.listeners.CommandListener') as mock_command_listener:
            # Mock "process_message" and use to verify that CommandListener
            # is called (when a file is created in the inbox).
            mock_method = mock_command_listener.process_message

            with inbox_watcher(inbox_dirname, mock_command_listener):
                create_test_file(inbox_dirname)

                wait_until_called(mock_method)

                mock_method.assert_called_once()

# Utility methods


@contextmanager
def inbox_watcher(inbox_dirname, mock_command_listener):
    inbox = MessageBox(inbox_dirname, PlastronMessage)
    watcher = InboxWatcher(mock_command_listener, inbox)
    watcher.start()

    try:
        yield
    finally:
        watcher.stop()


def create_test_file(inbox_dirname):
    temp_file = open(os.path.join(inbox_dirname, 'test_new_file'), 'w')
    temp_file.write("test:test")
    temp_file.close()


def wait_until_called(mock_method, interval=0.1, timeout=5):
    '''Polls at the given interval until either the mock_method is called
       or the timeout occurs.'''
    # Inspired by https://stackoverflow.com/a/36040926
    start = time.time()
    while not mock_method.called and time.time() - start < timeout:
        time.sleep(interval)
