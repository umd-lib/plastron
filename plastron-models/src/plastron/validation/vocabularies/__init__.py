import logging
from os.path import abspath, dirname
from pathlib import Path
from typing import Set
from urllib.error import HTTPError

from rdflib import Graph
from rdflib.term import Node

from plastron.validation import ValidationError

logger = logging.getLogger(__name__)

# Enable VOCABULARIES_DIR and VOCABULARIES to be overridden for tests
if 'VOCABULARIES_DIR' not in globals():
    VOCABULARIES_DIR = Path(dirname(abspath(__file__)))

if 'VOCABULARIES' not in globals():
    VOCABULARIES = {
        'http://purl.org/dc/dcmitype/': 'dcmitype.ttl',
    }


def get_vocabulary(vocab_uri: str) -> Graph:
    graph = Graph()
    # first check locally available vocabularies
    if vocab_uri in VOCABULARIES:
        try:
            graph.parse(
                location=str(VOCABULARIES_DIR / VOCABULARIES[vocab_uri]),
                format='turtle'
            )
            return graph
        except FileNotFoundError as e:
            logger.warning(f'Local version of {vocab_uri} not found: {e}')
            logger.info('Falling back to remote retrieval')

    # otherwise, fall back to remote retrieval
    try:
        graph.parse(location=vocab_uri)
    except HTTPError as e:
        raise ValidationError(f'Unable to retrieve vocabulary from {vocab_uri}: {e}') from e

    return graph


def get_subjects(vocab_uri: str) -> Set[Node]:
    subjects = set(get_vocabulary(vocab_uri).subjects())
    return subjects
