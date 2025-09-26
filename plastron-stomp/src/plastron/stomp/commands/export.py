import logging
from typing import Generator, Any

from plastron.context import PlastronContext
from plastron.jobs.exportjob import ExportJob
from plastron.messaging.messages import PlastronCommandMessage
from plastron.utils import strtobool

logger = logging.getLogger(__name__)


def export(
        context: PlastronContext,
        message: PlastronCommandMessage,
) -> Generator[dict[str, Any], None, dict[str, Any]]:
    ssh_key = context.config.get('COMMANDS', {}).get('EXPORT', {}).get('SSH_PRIVATE_KEY', None)
    export_job = ExportJob(
        context=context,
        export_binaries=bool(strtobool(message.args.get('export-binaries', 'false'))),
        binary_types=message.args.get('binary-types'),
        uris=message.body.strip().split('\n'),
        export_format=message.args.get('format', 'text/turtle'),
        output_dest=message.args.get('output-dest'),
        uri_template=message.args.get('uri-template'),
        key=ssh_key,
    )
    logger.info(f'Received message to initiate export job {message.job_id} containing {len(export_job.uris)} items')
    return export_job.run()
