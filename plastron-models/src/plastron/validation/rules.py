import re
from typing import Callable

from plastron.validation.vocabularies import get_subjects


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
    # is the set of property values a subset of the set of possible values?
    return set(prop.values) <= set(values)


def from_vocabulary(prop, vocab_uri):
    return allowed_values(prop, get_subjects(vocab_uri))


def value_pattern(prop, pattern):
    return not any(True for v in prop.values if not re.search(pattern, v))


def function(prop, func: Callable):
    return not any(True for v in prop.values if not func(v))
