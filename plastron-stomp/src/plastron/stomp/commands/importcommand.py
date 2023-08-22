import io
import logging
from argparse import Namespace, ArgumentTypeError

from plastron.jobs import ImportJob
from plastron.utils import datetimestamp
from plastron.namespaces import get_manager
from plastron.rdf import uri_or_curie
from plastron.repo import Repository

nsm = get_manager()
logger = logging.getLogger(__name__)


class Command:
    def __init__(self, config=None):
        self.config = config or {}
        self.result = None
        self.ssh_private_key = self.config.get('SSH_PRIVATE_KEY')
        self.jobs_dir = self.config.get('JOBS_DIR', 'jobs')

    def __call__(self, *args, **kwargs):
        for _ in self.execute(*args, **kwargs):
            pass

    @staticmethod
    def parse_message(message):
        access = message.args.get('access')
        message.body = message.body.encode('utf-8').decode('utf-8-sig')
        if access is not None:
            try:
                access_uri = uri_or_curie(access)
            except ArgumentTypeError as e:
                raise RuntimeError(f'PlastronArg-access {e}')
        else:
            access_uri = None
        return Namespace(
            model=message.args.get('model'),
            limit=message.args.get('limit', None),
            percentage=message.args.get('percent', None),
            validate_only=message.args.get('validate-only', False),
            resume=message.args.get('resume', False),
            import_file=io.StringIO(message.body),
            template_file=None,
            access=access_uri,
            member_of=message.args.get('member-of'),
            binaries_location=message.args.get('binaries-location'),
            container=message.args.get('container', None),
            extract_text_types=message.args.get('extract-text', None),
            job_id=message.job_id,
            structure=message.args.get('structure', None),
            relpath=message.args.get('relpath', None)
        )

    def execute(self, repo: Repository, args):
        """
        Performs the import

        :param repo: the repository configuration
        :param args: the command-line arguments
        """
        if args.resume and args.job_id is None:
            raise RuntimeError('Resuming a job requires a job id')

        if args.job_id is None:
            # TODO: generate a more unique id? add in user and hostname?
            args.job_id = f"import-{datetimestamp()}"

        operation = ImportJob(
            job_id=args.job_id,
            jobs_dir=self.jobs_dir,
            repo=repo,
            ssh_private_key=self.ssh_private_key,
        )
        if args.resume:
            metadata = yield from operation.resume()
        else:
            metadata = yield from operation.start(
                import_file=args.import_file,
                model=args.model,
                access=args.access,
                member_of=args.member_of,
                container=args.container,
                binaries_location=args.binaries_location,
            )

        """ TODO: statistics object to return
        logger.info(f'Skipped {metadata.skipped} items')
        logger.info(f'Completed {len(job.completed_log) - initial_completed_item_count} items')
        logger.info(f'Dropped {len(import_run.invalid_items)} invalid items')
        logger.info(f'Dropped {len(import_run.failed_items)} failed items')

        logger.info(f"Found {metadata.valid} valid items")
        logger.info(f"Found {metadata.invalid} invalid items")
        logger.info(f"Found {metadata.errors} errors")
        if not args.validate_only:
            logger.info(f"{metadata.unchanged} of {metadata.total} items remained unchanged")
            logger.info(f"Created {metadata.created} of {metadata.total} items")
            logger.info(f"Updated {metadata.updated} of {metadata.total} items")
        """
        if args.validate_only:
            # validate phase
            if metadata.invalid == 0:
                result_type = 'validate_success'
            else:
                result_type = 'validate_failed'
        else:
            # import phase
            if len(operation.job.completed_log) == metadata.total:
                result_type = 'import_complete'
            else:
                result_type = 'import_incomplete'

        self.result = {
            'type': result_type,
            'validation': metadata.validation_reports,
            'count': metadata.stats()
        }
