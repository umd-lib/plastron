import io
import logging
from argparse import ArgumentTypeError
from typing import Any, Generator, Optional

from rdflib import URIRef

from plastron.context import PlastronContext
from plastron.jobs import Jobs
from plastron.jobs.importjob import ImportConfig, ImportJob
from plastron.messaging.messages import PlastronCommandMessage
from plastron.utils import datetimestamp, strtobool, uri_or_curie

logger = logging.getLogger(__name__)


def get_access_uri(access) -> Optional[URIRef]:
    if access is None:
        return None
    try:
        return uri_or_curie(access)
    except ArgumentTypeError as e:
        raise RuntimeError(f'PlastronArg-access {e}')


def importcommand(
        context: PlastronContext,
        message: PlastronCommandMessage,
) -> Generator[dict[str, Any], None, dict[str, Any]]:
    """
    Performs the import

    :param context
    :param message:
    """
    job_id = message.job_id

    # per-request options that are NOT saved to the config
    limit = message.args.get('limit', None)
    if limit is not None:
        limit = int(limit)
    message.body = message.body.encode('utf-8').decode('utf-8-sig')
    percentage = message.args.get('percent', None)
    validate_only = bool(strtobool(message.args.get('validate-only', 'false')))
    publish = bool(strtobool(message.args.get('publish', 'false')))
    resume = bool(strtobool(message.args.get('resume', 'false')))
    import_file = io.StringIO(message.body)

    # options that are saved to the config
    job_config_args = {
        'job_id': job_id,
        'model': message.args.get('model'),
        'access': get_access_uri(message.args.get('access')),
        'member_of': message.args.get('member-of'),
        'container': message.args.get('relpath'),
        'binaries_location': message.args.get('binaries-location'),
    }

    if resume and job_id is None:
        raise RuntimeError('Resuming a job requires a job id')

    if job_id is None:
        # TODO: generate a more unique id? add in user and hostname?
        job_id = f"import-{datetimestamp()}"

    config = context.config.get('COMMANDS', {}).get('IMPORT', {})
    jobs = Jobs(directory=config.get('JOBS_DIR', 'jobs'))
    if resume:
        job = jobs.get_job(ImportJob, job_id=job_id)
        # update the config with any changes in this request
        job.update_config(job_config_args)
    else:
        job = jobs.create_job(ImportJob, config=ImportConfig(**job_config_args))

    job.ssh_private_key = config.get('SSH_PRIVATE_KEY', None)

    return job.run(
        context=context,
        import_file=import_file,
        limit=limit,
        percentage=percentage,
        validate_only=validate_only,
        publish=publish,
    )
