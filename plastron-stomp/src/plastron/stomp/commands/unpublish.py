from collections.abc import Generator
from typing import Any

from plastron.context import PlastronContext
from plastron.jobs.publicationjob import PublicationAction, PublicationJob
from plastron.messaging.messages import PlastronCommandMessage
from plastron.utils import strtobool


def unpublish(
    context: PlastronContext,
    message: PlastronCommandMessage,
) -> Generator[dict[str, Any], None, dict[str, Any]]:
    job = PublicationJob(
        context=context,
        uris=message.body.strip().split('\n'),
        action=PublicationAction.UNPUBLISH,
        force_hidden=bool(strtobool(message.args.get('hidden', 'false'))),
        force_visible=bool(strtobool(message.args.get('visible', 'false'))),
    )
    return job.run()
