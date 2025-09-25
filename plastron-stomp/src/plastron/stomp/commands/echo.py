import logging
import time
from typing import Generator, Any

from plastron.messaging.messages import PlastronCommandMessage
from plastron.repo import Repository

logger = logging.getLogger(__name__)


def echo(
        _repo: Repository,
        _config: dict[str, Any],
        message: PlastronCommandMessage,
) -> Generator[Any, None, dict[str, Any]]:
    message_body = message.body.encode('utf-8').decode('utf-8-sig')
    echo_delay = int(message.args.get('echo-delay', "0"))
    if echo_delay:
        time.sleep(echo_delay)

    yield {'echo': message_body}

    return {
        'type': 'Done',
        'echo': message_body,
    }
