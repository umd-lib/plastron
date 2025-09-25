import logging
from collections import defaultdict
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Mapping, ItemsView, Type, Iterable, Any, Generator

from pyparsing import ParseException
from rdflib import URIRef

from plastron.client import ClientError
from plastron.jobs.logs import AppendableSequence, NullLog
from plastron.namespaces import dcterms
from plastron.rdfmapping.resources import RDFResourceBase
from plastron.rdfmapping.validation import ValidationFailure
from plastron.repo import RepositoryResource, Repository
from plastron.repo.utils import context

logger = logging.getLogger(__name__)


class DryRun(Exception):
    pass


class UpdateError(Exception):
    pass


class ValidationFailed(Exception):
    def __init__(self, failures: Mapping[str, ValidationFailure] | ItemsView[str, ValidationFailure], *args):
        super().__init__(*args)
        self.failures = failures


def update(
        resource: RepositoryResource,
        sparql_update: str,
        model_class: Type[RDFResourceBase] = None,
        dry_run: bool = False,
) -> dict[str, str]:
    """Update a single resource using a SPARQL Update Query."""
    if model_class is not None:
        try:
            # Apply the update in-memory to the resource graph
            resource.graph.update(sparql_update)
        except ParseException as e:
            raise UpdateError(str(e)) from e

        # Validate the updated in-memory Graph using the model
        item = resource.describe(model_class)
        validation_result = item.validate()

        if not validation_result.ok:
            logger.warning(f'Resource {resource.url} failed validation')
            raise ValidationFailed(validation_result.failures())

    title = get_title_string(resource.graph)

    if dry_run:
        logger.info(f'Would update resource {resource} {title}')
        raise DryRun

    headers = {'Content-Type': 'application/sparql-update'}
    request_url = resource.description_url or resource.url
    try:
        response = resource.client.patch(request_url, data=sparql_update, headers=headers)
        if not response.ok:
            raise UpdateError(str(response))
    except ClientError as e:
        raise UpdateError(str(e)) from e

    logger.info(f'Updated resource {resource} {title}')
    timestamp = parsedate_to_datetime(response.headers['date']).isoformat('T')

    return {
        'uri': resource.url,
        'title': str(title),
        'timestamp': timestamp,
    }


@dataclass
class UpdateJob:
    repo: Repository
    uris: Iterable[str]
    sparql_update: str
    model_class: Type[RDFResourceBase]
    traverse: list[URIRef] = None
    completed: AppendableSequence = None
    dry_run: bool = False
    use_transactions: bool = True

    def run(self) -> Generator[dict[str, Any], None, dict[str, Any]]:
        if self.completed is None:
            self.completed = NullLog()

        logger.debug(
            f'SPARQL Update query:\n'
            f'====BEGIN====\n'
            f'{self.sparql_update}\n'
            f'=====END====='
        )
        if self.dry_run:
            logger.info('Dry run enabled, no actual updates will take place')

        stats = {
            'updated': [],
            'invalid': defaultdict(list),
            'errors': defaultdict(list)
        }
        for uri in self.uris:
            with context(repo=self.repo, use_transactions=self.use_transactions, dry_run=self.dry_run):
                for resource in self.repo[uri].walk(traverse=self.traverse):
                    if resource.url in self.completed:
                        logger.info(f'Resource {resource.url} has already been updated; skipping')
                        continue
                    try:
                        log_entry = update(
                            resource=resource,
                            sparql_update=self.sparql_update,
                            model_class=self.model_class,
                            dry_run=self.dry_run,
                        )
                        self.completed.append(log_entry)
                        stats['updated'].append(resource.url)
                    except DryRun:
                        # TODO: dry run should be implemented in the client
                        pass
                    except ValidationFailed as e:
                        stats['invalid'][resource.url].extend(f'{key}: {value}' for key, value in e.failures)
                    except UpdateError as e:
                        stats['errors'][resource.url].append(str(e))
                    yield stats

        if len(stats['errors']) == 0 and len(stats['invalid']) == 0:
            state = 'update_complete'
        else:
            state = 'update_incomplete'

        return {
            'type': state,
            'stats': stats
        }


def get_title_string(graph, separator='; '):
    return separator.join([t for t in graph.objects(predicate=dcterms.title)])
