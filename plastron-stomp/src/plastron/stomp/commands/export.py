import logging

from plastron.core.util import strtobool
from plastron.jobs import ExportJob

logger = logging.getLogger(__name__)


class Command:
    def __init__(self, config=None):
        self.config = config or {}
        self.ssh_private_key = self.config.get('SSH_PRIVATE_KEY')
        self.result = None

    def __call__(self, client, message):
        export_job = ExportJob(
            client=client,
            export_binaries=bool(strtobool(message.args.get('export-binaries', 'false'))),
            binary_types=message.args.get('binary-types'),
            uris=message.body.split('\n'),
            export_format=message.args.get('format', 'text/turtle'),
            output_dest=message.args.get('output-dest'),
            uri_template=message.args.get('uri-template'),
            key=self.ssh_private_key,
        )
        logger.info(f'Received message to initiate export job {message.job_id} containing {len(export_job.uris)} items')
        self.result = yield from export_job.run()
