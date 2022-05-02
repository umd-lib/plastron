import logging
from functools import lru_cache
from os.path import abspath, dirname
from pathlib import Path
from typing import List

import requests
from rdflib import Graph

from plastron.validation import ValidationError

logger = logging.getLogger(__name__)

VOCABULARIES_DIR = Path(dirname(abspath(__file__)))
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
    # LIBFCREPO-1093: use requests to fetch the vocab_uri, since rdflib < 6.0.0
    # does not support "308 Permanent Redirect" responses as redirection and
    # instead treats them as errors. This is fixed in rdflib 6.0.0, but that
    # requires Python > 3.7, and Plastron currently uses 3.6
    response = requests.get(vocab_uri, headers={'Accept': 'application/ld+json, text/turtle'})
    if not response.ok:
        raise ValidationError(f'Unable to retrieve vocabulary from {vocab_uri}: {response}')
    graph.parse(data=response.text, format=response.headers['Content-Type'])

    return graph


@lru_cache()
def get_subjects(vocab_uri: str) -> List[str]:
    return [str(s) for s in set(get_vocabulary(vocab_uri).subjects())]
