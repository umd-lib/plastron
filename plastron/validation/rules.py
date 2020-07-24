import re

from functools import lru_cache
from rdflib import Graph
from typing import Callable


def non_empty(values):
    return [v for v in values if len(str(v).strip()) > 0]


def required(prop, is_required=True):
    return len(non_empty(prop)) > 0 if is_required else True


def min_values(prop, length):
    return len(prop) >= length


def max_values(prop, length):
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


def function(prop, func: Callable):
    return not any(True for v in prop.values if not func(v))
