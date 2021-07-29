import logging
import urllib.parse
from pathlib import Path

from flask import Flask, url_for
from werkzeug.exceptions import InternalServerError, NotFound

from plastron.jobs import ImportJob
from plastron.util import ItemLog


logger = logging.getLogger(__name__)


def job_url(job_id):
    return url_for('show_job', _external=True, job_id=job_id)


def get_dropped_logs(job_dir):
    dropped_fieldnames = ['id', 'timestamp', 'title', 'uri', 'reason']
    return {f.name[f.name.index('-') + 1:f.name.index('.')]: ItemLog(f, dropped_fieldnames, 'id')
            for f in Path(job_dir).iterdir() if f.name.startswith('dropped-')}


def completed_items(job):
    return {
        'count': len(job.completed_log),
        'items': [c for c in job.completed_log]
    }


def create_app(config):
    app = Flask(__name__)
    app.config.from_mapping(config)
    jobs_dir: Path = app.config['JOBS_DIR']

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
        job.load_config()
        try:
            return {
                '@id': job_url(job_id),
                **job.config,
                'completed': completed_items(job),
                'total': job.metadata().total
            }
        except FileNotFoundError as e:
            raise InternalServerError from e

    @app.route('/jobs/<path:job_id>/completed')
    def show_completed_items(job_id):
        job = get_job(job_id)
        job.load_config()
        try:
            return completed_items(job), {'Content-Type': 'application/json'}
        except FileNotFoundError as e:
            raise InternalServerError from e

    return app
