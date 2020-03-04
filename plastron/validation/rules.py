import re
from functools import lru_cache

from plastron.namespaces import rdf, rdfs
from rdflib import Graph


vocab_cache = {}


def non_empty(values):
    return [v for v in values if len(v.strip()) > 0]


def required(prop):
    return min_len(prop, 1)


def min_len(prop, length):
    return len(prop) >= length


def max_len(prop, length):
    return len(prop) <= length


def exactly(prop, length):
    return len(prop) == length


def allowed_values(prop, values):
    return not any(True for v in prop.values if str(v) not in values)


@lru_cache()
def get_subjects(vocab_uri):
    graph = Graph().parse(vocab_uri)
    return [str(s) for s in set(graph.subjects())]


def from_vocabulary(prop, vocab_uri):
    return allowed_values(prop, get_subjects(vocab_uri))


def value_pattern(prop, pattern):
    return not any(True for v in prop.values if not re.search(pattern, v))
