""".. include:: ../../../docs/CSVSerializer.md"""
import logging

from rdflib import URIRef, Graph

from plastron.models import ContentModeledResource
from plastron.models.letter import Letter
from plastron.models.newspaper import Issue
from plastron.models.poster import Poster
from plastron.models.umd import Item
from plastron.namespaces import bibo, rdf, umd
from plastron.serializers.csv import CSVSerializer
from plastron.serializers.turtle import TurtleSerializer

logger = logging.getLogger(__name__)

SERIALIZER_CLASSES = {
    'text/turtle': TurtleSerializer,
    'turtle': TurtleSerializer,
    'ttl': TurtleSerializer,
    'text/csv': CSVSerializer,
    'csv': CSVSerializer
}

MODEL_MAP = {
    umd.Issue: Issue,
    umd.Item: Item,
    bibo.Image: Poster,
    bibo.Issue: Issue,
    bibo.Letter: Letter
}


def detect_resource_class(
    graph: Graph,
    subject: str | URIRef,
    fallback: type[ContentModeledResource] = None,
) -> type[ContentModeledResource]:
    types = set(graph.objects(URIRef(subject), rdf.type))

    for rdf_type, cls in MODEL_MAP.items():
        if rdf_type in types:
            return cls
    else:
        if fallback is not None:
            return fallback
        else:
            raise RuntimeError(f'Unable to detect resource type for {subject}')
