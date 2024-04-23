import logging
from collections.abc import Mapping
from os.path import abspath, dirname
from pathlib import Path
from typing import Union, Dict, ItemsView
from urllib.error import HTTPError

from rdflib import Graph
from rdflib.term import URIRef

from plastron.validation import ValidationError

logger = logging.getLogger(__name__)

# Enable VOCABULARIES_DIR and VOCABULARIES to be overridden for tests
if 'VOCABULARIES_DIR' not in globals():
    VOCABULARIES_DIR = Path(dirname(abspath(__file__)))

if 'VOCABULARIES' not in globals():
    VOCABULARIES = {
        URIRef('http://purl.org/dc/dcmitype/'): 'dcmitype.ttl',
    }


class Vocabulary(Mapping):
    """Class representing an RDF vocabulary. Implements the `Mapping` abstract
    base class, so you can do all the following:

    ```pycon
    >>> vocab = Vocabulary('http://purl.org/dc/dcmitype/')

    >>> str(vocab)
    ''http://purl.org/dc/dcmitype/''

    >>> len(vocab)
    12

    >>> for term_uri in vocab:
    ...     print(term_uri)
    http://purl.org/dc/dcmitype/StillImage
    http://purl.org/dc/dcmitype/MovingImage
    http://purl.org/dc/dcmitype/Service
    http://purl.org/dc/dcmitype/Sound
    http://purl.org/dc/dcmitype/PhysicalObject
    http://purl.org/dc/dcmitype/Software
    http://purl.org/dc/dcmitype/Text
    http://purl.org/dc/dcmitype/Dataset
    http://purl.org/dc/dcmitype/Collection
    http://purl.org/dc/dcmitype/Event
    http://purl.org/dc/dcmitype/Image
    http://purl.org/dc/dcmitype/InteractiveResourcee

    >>> 'Image' in vocab
    True

    >>> URIRef('http://purl.org/dc/dcmitype/Image') in vocab
    True
    ```

    Membership checks and item lookups can both either use the full `URIRef` object
    representing a term, or just the local string portion of the URI that comes after
    the base namespace URI.

    Item lookup takes a subject URI and returns a dictionary where the keys are predicate
    URIs and the values are the objects (Literals or URIs) for statements with the given
    subject URI.

    By making use of `plastron.namespaces`, it is easy to retrieve details from
    vocabulary terms:

    ```pycon
    >>> from plastron.namespaces import rdfs

    >>> vocab['Image'][rdfs.label]
    rdflib.term.Literal('Image', lang='en')

    >>> str(vocab['Image'][rdfs.label])
    'Image'

    >>> for term_uri, term in vocab.items():
    ...     print(term[rdfs.label])
    Still Image
    Moving Image
    Service
    Sound
    Physical Object
    Software
    Text
    Dataset
    Collection
    Event
    Image
    Interactive Resource
    ```

    `Vocabulary` objects can also be used in the `values_from` attribute of RDF
    property mapping fields.
    """
    def __init__(self, uri: Union[URIRef, str]):
        self.uri = URIRef(uri)

    @property
    def term_uris(self):
        return set(get_vocabulary_graph(self.uri).subjects()) - {self.uri}

    def _term_uri(self, item) -> URIRef:
        return item if isinstance(item, URIRef) else URIRef(self.uri + item)

    def __str__(self):
        return str(self.uri)

    def __len__(self) -> int:
        return len(self.term_uris)

    def __contains__(self, item) -> bool:
        return self._term_uri(item) in self.term_uris

    def __iter__(self):
        yield from self.term_uris

    def __getitem__(self, item):
        term_uri = self._term_uri(item)
        if term_uri not in self:
            raise KeyError(item)
        return {p: o for p, o in get_vocabulary_graph(self.uri).predicate_objects(term_uri)}

    def items(self) -> ItemsView[URIRef, Dict]:
        return {term_uri: self[term_uri] for term_uri in self}.items()


def get_vocabulary_graph(vocab_uri: URIRef) -> Graph:
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
