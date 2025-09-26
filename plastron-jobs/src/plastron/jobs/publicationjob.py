import logging
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import Generator, Any, Mapping

from plastron.context import PlastronContext
from plastron.jobs import Job
from plastron.repo import RepositoryError
from plastron.repo.publish import PublishableResource

logger = logging.getLogger(__name__)


class PublicationAction(Enum):
    PUBLISH = 'publish'
    UNPUBLISH = 'unpublish'

    @classmethod
    def get_final_state(cls, action: str | Enum, count: Mapping[str, int]):
        try:
            return cls(action).value + ('_incomplete' if count['done'] < count['total'] else '_complete')
        except ValueError as e:
            logger.error(str(e))
            return 'error'


@dataclass
class PublicationJob(Job):
    context: PlastronContext
    uris: list[str]
    action: PublicationAction
    force_hidden: bool = False
    force_visible: bool = False

    def run(self) -> Generator[dict[str, Any], None, dict[str, Any]]:
        count = Counter(
            total=len(self.uris),
            done=0,
            errors=0,
        )
        yield {
            'count': count,
            'state': 'publish_in_progress',
            'progress': 0,
        }
        for n, uri in enumerate(self.uris, 1):
            try:
                resource: PublishableResource = self.context.repo[uri:PublishableResource].read()

                if self.action == PublicationAction.PUBLISH:
                    handle = resource.publish(
                        handle_client=self.context.handle_client,
                        public_url=self.context.get_public_url(resource),
                        force_hidden=self.force_hidden,
                        force_visible=self.force_visible,
                    )
                    count['done'] += 1
                    result = {
                        'uri': uri,
                        'handle': str(handle),
                        'status': resource.publication_status,
                    }
                elif self.action == PublicationAction.UNPUBLISH:
                    resource.unpublish(
                        force_hidden=self.force_hidden,
                        force_visible=self.force_visible,
                    )
                    count['done'] += 1
                    result = {
                        'uri': uri,
                        'status': resource.publication_status,
                    }
                else:
                    logger.error(f'Unknown action: {self.action}')
                    count['errors'] += 1
                    result = {'error': f'Unknown action: {self.action}'}

            except RepositoryError as e:
                logger.error(str(e))
                result = {'error': str(e)}
                count['errors'] += 1

            yield {
                'count': count,
                'result': result,
                'state': 'publish_in_progress',
                'progress': int(n / count['total'] * 100)
            }

        state = PublicationAction.get_final_state(self.action, count)
        return {
            'type': state,
            'count': count,
            'state': state,
            'progress': 100,
        }
