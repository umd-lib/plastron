""".. include:: ../../../docs/serializers.md"""
import logging

from rdflib import URIRef

from plastron.models.letter import Letter
from plastron.models.newspaper import Issue
from plastron.models.poster import Poster
from plastron.namespaces import bibo, rdf
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
    bibo.Image: Poster,
    bibo.Issue: Issue,
    bibo.Letter: Letter
}


def detect_resource_class(graph, subject, fallback=None):
    types = set(graph.objects(URIRef(subject), rdf.type))

    for rdf_type, cls in MODEL_MAP.items():
        if rdf_type in types:
            return cls
    else:
        if fallback is not None:
            return fallback
        else:
            raise RuntimeError(f'Unable to detect resource type for {subject}')
