from unittest.mock import MagicMock

import pytest
from rdflib import URIRef

from plastron.context import PlastronContext
from plastron.models.fedora import FedoraResource
from plastron.repo import RepositoryResource


@pytest.mark.parametrize(
    ('public_url_pattern', 'resource_url', 'container_url', 'expected_public_url'),
    [
        (
            'http://digital-test/result/id/{uuid}',
            'http://fcrepo-test/fcrepo/rest/pcdm/f4/f0/46/77/f4f04677-6ebe-4166-b30d-232fd2ad4e10',
            'http://fcrepo-test/fcrepo/rest/pcdm',
            'http://digital-test/result/id/f4f04677-6ebe-4166-b30d-232fd2ad4e10',
        ),
        (
            'http://digital-test/result/id/{uuid}?relpath={container_path}',
            'http://fcrepo-test/fcrepo/rest/pcdm/f4/f0/46/77/f4f04677-6ebe-4166-b30d-232fd2ad4e10',
            'http://fcrepo-test/fcrepo/rest/pcdm',
            'http://digital-test/result/id/f4f04677-6ebe-4166-b30d-232fd2ad4e10?relpath=/pcdm',
        ),
        (
            'http://digital-test/result/id/{uuid}?relpath={relpath}',
            'http://fcrepo-test/fcrepo/rest/pcdm/f4/f0/46/77/f4f04677-6ebe-4166-b30d-232fd2ad4e10',
            'http://fcrepo-test/fcrepo/rest/pcdm',
            'http://digital-test/result/id/f4f04677-6ebe-4166-b30d-232fd2ad4e10?relpath=pcdm',
        ),
        (
            'http://digital-test/result/?path={path}',
            'http://fcrepo-test/fcrepo/rest/pcdm/f4/f0/46/77/f4f04677-6ebe-4166-b30d-232fd2ad4e10',
            'http://fcrepo-test/fcrepo/rest/pcdm',
            'http://digital-test/result/?path=/pcdm/f4/f0/46/77/f4f04677-6ebe-4166-b30d-232fd2ad4e10',
        ),
        (
            'http://digital-test/search/id/{iiif_id}',
            'http://fcrepo-test/fcrepo/rest/pcdm/f4/f0/46/77/f4f04677-6ebe-4166-b30d-232fd2ad4e10',
            'http://fcrepo-test/fcrepo/rest/pcdm',
            'http://digital-test/search/id/fcrepo:pcdm:f4:f0:46:77:f4f04677-6ebe-4166-b30d-232fd2ad4e10',
        ),
    ]
)
def test_get_public_url(public_url_pattern, resource_url, container_url, expected_public_url):
    resource = MagicMock(
        spec=RepositoryResource,
        url=resource_url,
    )
    resource.describe.return_value = FedoraResource(parent=URIRef(container_url))
    config = {
        'REPOSITORY': {
            'REST_ENDPOINT': 'http://fcrepo-test/fcrepo/rest',
        },
        'PUBLICATION_WORKFLOW': {
            'PUBLIC_URL_PATTERN': public_url_pattern,
        }
    }
    context = PlastronContext(config)
    assert context.get_public_url(resource) == expected_public_url
