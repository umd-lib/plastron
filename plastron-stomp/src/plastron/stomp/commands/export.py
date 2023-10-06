import logging
from typing import Generator, Any, Dict

from plastron.jobs.exportjob import ExportJob
from plastron.repo import Repository
from plastron.stomp.messages import PlastronCommandMessage
from plastron.utils import strtobool

logger = logging.getLogger(__name__)


def export(
        repo: Repository,
        config: Dict[str, Any],
        message: PlastronCommandMessage,
) -> Generator[Dict[str, str], None, Dict[str, Any]]:
    export_job = ExportJob(
        repo=repo,
        export_binaries=bool(strtobool(message.args.get('export-binaries', 'false'))),
        binary_types=message.args.get('binary-types'),
        uris=message.body.strip().split('\n'),
        export_format=message.args.get('format', 'text/turtle'),
        output_dest=message.args.get('output-dest'),
        uri_template=message.args.get('uri-template'),
        key=config.get('SSH_PRIVATE_KEY', None),
    )
    logger.info(f'Received message to initiate export job {message.job_id} containing {len(export_job.uris)} items')
    return export_job.run()
