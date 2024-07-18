from typing import Dict, Any, Generator

from plastron.context import PlastronContext
from plastron.jobs.publicationjob import PublicationJob, PublicationAction
from plastron.messaging.messages import PlastronCommandMessage
from plastron.utils import strtobool


def publish(
        context: PlastronContext,
        message: PlastronCommandMessage,
) -> Generator[Dict[str, Any], None, Dict[str, Any]]:
    job = PublicationJob(
        context=context,
        uris=message.body.strip().split('\n'),
        action=PublicationAction.PUBLISH,
        force_hidden=bool(strtobool(message.args.get('hidden', 'false'))),
        force_visible=bool(strtobool(message.args.get('visible', 'false'))),
    )
    return job.run()
