import csv
import logging
from argparse import FileType, ArgumentTypeError, Namespace
from typing import TextIO

from plastron.cli.commands import BaseCommand
from plastron.jobs.importjob.ndnp import NDNPBatch, write_import_csv
from plastron.jobs.importjob import ImportConfig, ImportJob
from plastron.jobs import Jobs
from plastron.models import get_model_from_name, ModelClassNotFoundError
from plastron.utils import datetimestamp, uri_or_curie

logger = logging.getLogger(__name__)


# custom argument type for percentage loads
def percentile(n):
    p = int(n)
    if not p > 0 and p < 100:
        raise ArgumentTypeError("Percent param must be 1-99")
    return p


def write_model_template(model_name: str, template_file: TextIO):
    try:
        model_class = get_model_from_name(model_name)
    except ModelClassNotFoundError as e:
        raise RuntimeError(f'Cannot find model class named {model_name}') from e
    if not hasattr(model_class, 'HEADER_MAP'):
        raise RuntimeError(f'{model_class.__name__} has no HEADER_MAP, cannot create template')

    logger.info(f'Writing template for the {model_class.__name__} model to {template_file.name}')
    writer = csv.writer(template_file)
    writer.writerow(parse_model_header_map(model_class) + ['FILES', 'ITEM_FILES'])


def parse_model_header_map(model_class):
    """
    Returns a list of fields to use as column headers for the CSV template file
    derived from the HEADER_MAP of the given model class.
    """
    header_fields = []

    for header in model_class.HEADER_MAP.values():
        if isinstance(header, dict):
            # Every value in a header map dictionary is assumed to be a field
            # name that should be used by the CSV file.
            for value in header.values():
                header_fields.append(value)
        else:
            header_fields.append(header)

    return header_fields


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='import',
        description='Import data to the repository'
    )
    parser.add_argument(
        '-m', '--model',
        help='data model to use',
        action='store'
    )
    parser.add_argument(
        '-l', '--limit',
        help='limit the number of rows to read from the import file',
        type=int,
        action='store'
    )
    parser.add_argument(
        '-%', '--percent',
        help=(
            'select an evenly spaced subset of items to import; '
            'the size of this set will be as close as possible '
            'to the specified percentage of the total items'
        ),
        type=percentile,
        dest='percentage',
        action='store'
    )
    parser.add_argument(
        '--validate-only',
        help='only validate, do not do the actual import',
        action='store_true'
    )
    parser.add_argument(
        '--make-template',
        help='create a CSV template for the given model',
        dest='template_file',
        metavar='FILENAME',
        type=FileType('w'),
        action='store'
    )
    parser.add_argument(
        '--convert-from',
        help='use a pre-processor to transform another data format into an import job',
        choices=['ndnp'],
        action='store',
    )
    parser.add_argument(
        '--convert-option', '-o',
        help='set a parameter to used by the --convert-from pre-processor; repeatable',
        dest='convert_params',
        nargs=2,
        metavar=('NAME', 'VALUE'),
        action='append',
    )
    parser.add_argument(
        '--access',
        help='URI or CURIE of the access class to apply to new items',
        type=uri_or_curie,
        metavar='URI|CURIE',
        action='store'
    )
    parser.add_argument(
        '--member-of',
        help='URI of the object that new items are PCDM members of',
        metavar='URI',
        action='store'
    )
    parser.add_argument(
        '--binaries-location',
        help=(
            'where to find binaries; either a path to a directory, '
            'a "zip:<path to zipfile>" URI, an SFTP URI in the form '
            '"sftp://<user>@<host>/<path to dir>", or a URI in the '
            'form "zip+sftp://<user>@<host>/<path to zipfile>"'
        ),
        metavar='LOCATION',
        action='store'
    )
    parser.add_argument(
        '--container',
        help=(
            'parent container for new items; defaults to the RELPATH '
            'in the repo configuration file'
        ),
        metavar='PATH',
        action='store'
    )
    parser.add_argument(
        '--job-id',
        help='unique identifier for this job; defaults to "import-{timestamp}"',
        action='store'
    )
    parser.add_argument(
        '--resume',
        help='resume a job that has been started; requires --job-id {id} to be present',
        action='store_true'
    )
    parser.add_argument(
        '--extract-text-from', '-x',
        help=(
            'extract text from binaries of the given MIME types, '
            'and add as annotations'
        ),
        dest='extract_text_types',
        metavar='MIME_TYPES',
        action='store'
    )
    parser.add_argument(
        '--publish',
        help='automatically publish all items in this import',
        action='store_true',
    )
    parser.add_argument(
        'import_file', nargs='?',
        help='name of the file to import from',
        type=FileType('r', encoding='utf-8-sig'),
        action='store'
    )
    parser.set_defaults(cmd_name='import')


def create_job_id() -> str:
    # TODO: generate a more unique id? add in user and hostname?
    return f"import-{datetimestamp()}"


class Command(BaseCommand):
    @property
    def jobs_dir(self):
        return self.config.get('JOBS_DIR', 'jobs')

    def __call__(self, args: Namespace):
        """
        Performs the import

        :param args: the command-line arguments
        """
        if hasattr(args, 'template_file') and args.template_file is not None:
            write_model_template(args.model, args.template_file)
            return

        jobs = Jobs(self.jobs_dir)

        if args.resume:
            if args.job_id is None:
                raise RuntimeError('Resuming a job requires a job id')

            logger.info(f'Resuming saved job {args.job_id}')
            job = jobs.get_job(ImportJob, args.job_id)
        else:
            if args.container is None:
                raise RuntimeError('A container is required unless resuming an existing job')

            if args.job_id is None:
                args.job_id = create_job_id()

            if args.convert_from is not None:
                if args.convert_from == 'ndnp':
                    params = dict(args.convert_params or [])
                    batch = NDNPBatch(params['dir'], params['batch_file'])
                    logger.info(f'Converting NDNP batch at {batch.batch_file} to import job {args.job_id}')
                    job = jobs.create_job(job_class=ImportJob, config=ImportConfig(
                        job_id=args.job_id,
                        model='Issue',
                        access=args.access,
                        member_of=args.member_of,
                        container=args.container,
                        binaries_location=str(batch.root_dir),
                    ))
                    with job.metadata_file.open(mode='w') as fh:
                        write_import_csv(batch, fh)
                else:
                    raise RuntimeError(f'Unrecognized import converter "{args.convert_from}"')
            else:
                logger.info(f'Creating new job {args.job_id}')

                if args.import_file is None:
                    raise RuntimeError('An import file is required unless resuming an existing job')

                if args.model is None:
                    raise RuntimeError('A model is required unless resuming an existing job')

                job = jobs.create_job(
                    job_class=ImportJob,
                    config=ImportConfig(
                        job_id=args.job_id,
                        model=args.model,
                        access=args.access,
                        member_of=args.member_of,
                        container=args.container,
                        binaries_location=args.binaries_location,
                    ),
                )

        logger.debug(f'Running job {job.id}')
        self.run(job.run(
            context=self.context,
            import_file=args.import_file,
            limit=args.limit,
            percentage=args.percentage,
            validate_only=args.validate_only,
            publish=args.publish,
        ))

        for key, value in self.result['count'].items():
            logger.info(f"{key.title().replace('_', ' ')}: {value}")
