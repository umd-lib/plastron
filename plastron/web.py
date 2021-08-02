import logging
import urllib.parse
from pathlib import Path

from flask import Flask, url_for
from werkzeug.exceptions import InternalServerError, NotFound

from plastron.jobs import ConfigMissingError, ImportJob, JobError

logger = logging.getLogger(__name__)


def job_url(job_id):
    return url_for('show_job', _external=True, job_id=job_id)


def items(log):
    return {
        'count': len(log),
        'items': [c for c in log]
    }


def latest_dropped_items(job: ImportJob):
    latest_run = job.latest_run()
    if latest_run is None:
        return {}

    return {
        'timestamp': latest_run.timestamp,
        'failed': items(latest_run.failed_items),
        'invalid': items(latest_run.invalid_items)
    }


def create_app(config):
    app = Flask(__name__)
    app.config.from_mapping(config)
    jobs_dir = Path(app.config['JOBS_DIR'])

    def get_job(job_id: str):
        job = ImportJob(urllib.parse.unquote(job_id), str(jobs_dir))
        if not job.dir_exists:
            raise NotFound
        return job

    @app.route('/jobs')
    def list_jobs():
        if not jobs_dir.exists():
            logger.warning(f'Jobs directory "{jobs_dir.absolute()}" does not exist; returning empty list')
            return {'jobs': []}
        job_ids = sorted(f.name for f in jobs_dir.iterdir() if f.is_dir())
        return {'jobs': [{'@id': job_url(job_id), 'job_id': job_id} for job_id in job_ids]}

    @app.route('/jobs/<path:job_id>')
    def show_job(job_id):
        job = get_job(job_id)
        try:
            job.load_config()
        except ConfigMissingError as e:
            logger.warning(f'Cannot open config file {job.config_filename} for job {job}')
            # TODO: more complete information in the response body?
            raise NotFound

        try:
            return {
                '@id': job_url(job_id),
                **job.config,
                'runs': job.runs,
                'completed': items(job.completed_log),
                'dropped': latest_dropped_items(job),
                'total': job.metadata().total
            }
        except JobError as e:
            raise NotFound from e

    return app
