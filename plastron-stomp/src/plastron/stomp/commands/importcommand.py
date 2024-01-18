import io
import logging
from argparse import ArgumentTypeError
from typing import Generator, Any, Dict

from plastron.jobs.imports import ImportJobs, ImportConfig
from plastron.rdf import uri_or_curie
from plastron.repo import Repository
from plastron.stomp.messages import PlastronCommandMessage
from plastron.utils import datetimestamp

logger = logging.getLogger(__name__)


def importcommand(
    repo: Repository,
    config: Dict[str, Any],
    message: PlastronCommandMessage,
) -> Generator[Any, None, Dict[str, Any]]:
    """
    Performs the import

    :param repo: the repository configuration
    :param config:
    :param message:
    """
    limit = message.args.get('limit', None)
    if limit is not None:
        limit = int(limit)
    access = message.args.get('access')
    message.body = message.body.encode('utf-8').decode('utf-8-sig')
    if access is not None:
        try:
            access_uri = uri_or_curie(access)
        except ArgumentTypeError as e:
            raise RuntimeError(f'PlastronArg-access {e}')
    else:
        access_uri = None
    model = message.args.get('model')
    percentage = message.args.get('percent', None)
    validate_only = message.args.get('validate-only', False)
    resume = message.args.get('resume', False)
    import_file = io.StringIO(message.body)
    member_of = message.args.get('member-of')
    binaries_location = message.args.get('binaries-location')
    container = message.args.get('container', None)
    # extract_text_types = message.args.get('extract-text', None)
    job_id = message.job_id
    # structure = message.args.get('structure', None)
    # relpath = message.args.get('relpath', None)

    if resume and args.job_id is None:
        raise RuntimeError('Resuming a job requires a job id')

    if job_id is None:
        # TODO: generate a more unique id? add in user and hostname?
        job_id = f"import-{datetimestamp()}"

    jobs = ImportJobs(directory=config.get('JOBS_DIR', 'jobs'))
    if resume:
        job = jobs.get_job(job_id=job_id)
        job.ssh_private_key = config.get('SSH_PRIVATE_KEY', None)
    else:
        job = jobs.create_job(config=ImportConfig(
            job_id=job_id,
            model=model,
            access=access_uri,
            member_of=member_of,
            container=container,
            binaries_location=binaries_location,
        ))

    return job.run(
        repo=repo,
        import_file=import_file,
        limit=limit,
        percentage=percentage,
        validate_only=validate_only,
    )
