import logging
from collections.abc import Mapping
from os.path import abspath, dirname
from pathlib import Path
from typing import ItemsView
from urllib.error import HTTPError, URLError

from rdflib import Graph
from rdflib.term import URIRef, Literal

from plastron.namespaces import rdfs, dcterms, rdf, owl
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.graph import TrackChangesGraph
from plastron.rdfmapping.resources import RDFResource

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
    def __init__(self, uri: URIRef | str):
        self.uri = URIRef(uri)

    @property
    def term_uris(self):
        """Returns the set of URIs for all the terms in this vocabulary."""
        return set(get_vocabulary_graph(self.uri).subjects()) - {self.uri}

    def _term_uri(self, item) -> URIRef:
        return item if isinstance(item, URIRef) else URIRef(self.uri + item)

    def term_graph(self, term: URIRef | str) -> TrackChangesGraph:
        """Returns a graph containing all the triples with the given `term`
        as their subject."""
        graph = TrackChangesGraph()
        for triple in get_vocabulary_graph(self.uri).triples((self._term_uri(term), None, None)):
            graph.add(triple)
        return graph

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

    def items(self) -> ItemsView[URIRef, dict]:
        """Returns an `ItemsView` mapping each term URI to a dictionary mapping
        of predicates to objects. Allows for easy iteration over the vocabulary
        as if it were a dictionary."""
        return {term_uri: self[term_uri] for term_uri in self}.items()

    def find(self, p: URIRef, o: URIRef | Literal) -> dict:
        """Finds the first term with a triple matching the given predicate `p`
        and object `o` in the vocabulary. Raises a `KeyError` if no such term
        can be found."""
        for _, term in self.items():
            if o in term[p]:
                return term
        else:
            raise KeyError(f'{p} {o}')


class VocabularyTerm(RDFResource):
    vocab: Vocabulary = None
    label = DataProperty(rdfs.label, required=True)
    description = DataProperty(dcterms.description)
    value = DataProperty(rdf.value)
    comment = DataProperty(rdfs.comment)
    same_as = ObjectProperty(owl.sameAs, repeatable=True)

    @classmethod
    def from_vocab(cls, vocab: Vocabulary, name: str = None) -> type:
        """Create a new subclass of `VocabularyTerm` with the `vocab` property
        set to the given `Vocabulary`. If `name` is not given, a name for the
        new class is generated by taking the last segment of the vocabulary
        URI and ensuring it starts with an uppercase letter.

        ```pycon
        >>> from plastron.validation.vocabularies import Vocabulary, VocabularyTerm

        >>> formats = Vocabulary('http://vocab.lib.umd.edu/form#')

        >>> new_cls = VocabularyTerm.from_vocab(formats)

        >>> new_cls.__name__
        'FormVocabularyTerm'

        >>> new_cls_with_name = VocabularyTerm.from_vocab(formats, 'UMDFormatsTerm')

        >>> new_cls_with_name.__name__
        'UMDFormatsTerm'
        ```
        """
        if name is None:
            vocab_name = vocab.uri.rstrip('/#').rsplit('/', 1)[1]
            name = vocab_name[0].upper() + vocab_name[1:] + 'VocabularyTerm'

        return type(name, (VocabularyTerm,), {'vocab': vocab})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.vocab is not None:
            # we just want the part with the term as the subject
            self._graph = self.vocab.term_graph(self.uri)


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
    except (URLError, HTTPError) as e:
        raise RuntimeError(f'Unable to retrieve vocabulary from {vocab_uri}: {e}') from e

    return graph


class ControlledVocabularyProperty(ObjectProperty):
    """Specialized subclass of `plastron.rdfmapping.descriptors.ObjectProperty`
    for object properties whose values should come from a single controlled
    vocabulary. The following two property declarations are equivalent:

    ```python
    object_type = ControlledVocabularyProperty(
        dcterms.type,
        required=True,
        vocab=DCMI_TYPES,
    )

    object_type = ObjectProperty(
        dcterms.type,
        required=True,
        values_from=DCMI_TYPES,
        cls=VocabularyTerm.from_vocab(DCMI_TYPES),
    )
    ```
    """
    def __init__(self, predicate: URIRef, vocab: Vocabulary, **kwargs):
        super().__init__(
            predicate,
            **kwargs,
            values_from=vocab,
            cls=VocabularyTerm.from_vocab(vocab),
        )
