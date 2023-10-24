import logging
from dataclasses import dataclass, field
from typing import List

from bs4 import BeautifulSoup

from plastron.files import BinarySource
from plastron.namespaces import sc
from plastron.models.annotations import FullTextAnnotation, TextualBody

logger = logging.getLogger(__name__)


class JobError(Exception):
    def __init__(self, job, *args):
        super().__init__(*args)
        self.job = job

    def __str__(self):
        return f'Job {self.job} error: {super().__str__()}'


class JobConfigError(JobError):
    pass


def annotate_from_files(item, mime_types):
    for member in item.has_member.objects:
        # extract text from HTML files
        for file in filter(lambda f: str(f.mimetype) in mime_types, member.has_file.objects):
            if str(file.mimetype) == 'text/html':
                # get text from HTML
                with file.source as stream:
                    text = BeautifulSoup(b''.join(stream), features='lxml').get_text()
            else:
                logger.warning(f'Extracting text from {file.mimetype} is not supported')
                continue

            annotation = FullTextAnnotation(
                target=member,
                body=TextualBody(value=text, content_type='text/plain'),
                motivation=sc.painting,
                derived_from=file
            )
            # don't embed full resources
            annotation.props['target'].is_embedded = False

            member.annotations.append(annotation)


@dataclass
class FileSpec:
    name: str
    source: BinarySource = None

    def __str__(self):
        return self.name


@dataclass
class FileGroup:
    rootname: str
    files: List[FileSpec] = field(default_factory=list)

    def __str__(self):
        extensions = list(map(lambda f: str(f).replace(self.rootname, ''), self.files))
        return f'{self.rootname}{{{",".join(extensions)}}}'
