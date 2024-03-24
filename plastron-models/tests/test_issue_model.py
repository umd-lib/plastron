from plastron.models.newspaper import Issue
from plastron.namespaces import ore
from rdflib import Graph, URIRef

import pytest

base_uri = 'http://example.com/xyz'


@pytest.mark.skip('Test cannot be run because Issue validation does not currently work')
def test_issue_valid_with_only_required_fields():
    issue = Issue()

    # Only provide required fields
    issue.identifier = 'test_issue'
    issue.title = 'Test Issue'
    issue.date = '1970-01-01'
    issue.volume = '1'
    issue.issue = '1'
    issue.edition = '1'

    assert issue.is_valid
