import dataclasses
import logging
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Type, TypeVar

import yaml

from plastron.jobs.logs import ItemLog

logger = logging.getLogger(__name__)


def is_run_dir(path: Path) -> bool:
    return path.is_dir() and re.match(r'^\d{14}$', path.name)


@dataclass
class JobConfig:
    job_id: str

    @classmethod
    def from_file(cls, filename: str | Path):
        try:
            with open(filename) as file:
                config = yaml.safe_load(file)
        except FileNotFoundError as e:
            raise JobConfigError(f'Config file {filename} is missing') from e
        if config is None:
            raise JobConfigError(f'Config file {filename} is empty')
        for key, value in config.items():
            # catch any improperly serialized "None" values, and convert to None
            if value == 'None':
                config[key] = None
        return cls(**config)

    def save(self, filename: str | Path):
        config = {k: str(v) if v is not None else v for k, v in vars(self).items()}
        with open(filename, mode='w') as file:
            yaml.dump(data=config, stream=file)


class Job:
    run_class = None
    config_class = None

    def __init__(self, job_id: str, job_dir: Path):
        self.id = job_id
        self.dir = job_dir
        self.config = None
        # record of items that are successfully loaded
        completed_fieldnames = ['id', 'timestamp', 'title', 'uri', 'status']
        self.completed_log = ItemLog(self.dir / 'completed.log.csv', completed_fieldnames, 'id')

    def __str__(self):
        return self.id

    @property
    def config_filename(self) -> Path:
        return self.dir / 'config.yml'

    @property
    def exists(self) -> bool:
        return self.dir.is_dir()

    def load_config(self):
        self.config = self.config_class.from_file(self.config_filename)
        return self

    def update_config(self, job_config_args: dict[str, Any]):
        """Update the config with values from `job_config_args` that are not `None`."""
        self.config = dataclasses.replace(self.config, **{k: v for k, v in job_config_args.items() if v is not None})
        return self

    def new_run(self):
        return self.run_class(self)

    def get_run(self, timestamp: Optional[str] = None):
        if timestamp is None:
            # get the latest run
            return self.latest_run()
        else:
            return self.run_class(self).load(timestamp)

    @property
    def runs(self) -> list[str]:
        return sorted((d.name for d in filter(is_run_dir, self.dir.iterdir())), reverse=True)

    def latest_run(self):
        try:
            return self.run_class(self).load(self.runs[0])
        except IndexError:
            return None


class Jobs:
    J = TypeVar('J')
    C = TypeVar('C')

    def __init__(self, directory: Path | str):
        self.dir = Path(directory)

    def create_job(self, job_class: Type[J], job_id: str = None, config: C = None) -> J:
        if config is None:
            if job_id is None:
                raise RuntimeError('Must specify either a job_id or config')
            config = job_class.config_class(job_id=job_id)
        if type(config) is not job_class.config_class:
            raise TypeError(
                f'Provided config class "{type(config).__name__}" '
                f'differs from the expected config class for "{job_class.__name__}": '
                f'"{job_class.config_class.__name__}"'
            )
        safe_id = urllib.parse.quote(config.job_id, safe='')
        job_dir = self.dir / safe_id
        if job_dir.exists():
            raise RuntimeError(f'Job directory {job_dir} for job id {config.job_id} already exists')
        job_dir.mkdir(parents=True, exist_ok=True)
        config.save(job_dir / 'config.yml')
        logger.info(f'Created job with id {config.job_id}')
        return job_class(job_id=config.job_id, job_dir=job_dir).load_config()

    def get_job(self, job_class: Type[J], job_id: str) -> J:
        safe_id = urllib.parse.quote(job_id, safe='')
        job_dir = self.dir / safe_id
        if not job_dir.exists():
            raise JobNotFoundError(f'Job directory {job_dir} for job id {job_id} does not exist')
        return job_class(job_id=job_id, job_dir=job_dir).load_config()


class JobError(Exception):
    def __init__(self, job, *args):
        super().__init__(*args)
        self.job = job

    def __str__(self):
        return f'Job {self.job} error: {super().__str__()}'


class JobConfigError(JobError):
    pass


class JobNotFoundError(JobError):
    pass
