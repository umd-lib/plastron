from edtf import parse_edtf
from iso639 import is_valid639_1, is_valid639_2
from pyparsing import ParseException


def is_edtf_formatted(value):
    try:
        parse_edtf(value)
        return True
    except ParseException:
        return False


def is_valid_iso639_code(value):
    return is_valid639_1(value) or is_valid639_2(value)


class ResourceValidationResult:
    def __init__(self, resource):
        self.resource = resource
        self.outcomes = []

    def __bool__(self):
        return self.is_valid()

    def is_valid(self):
        return len(list(self.failed())) == 0

    def passes(self, prop, rule, expected):
        self.outcomes.append((prop, 'passed', rule, expected))

    def fails(self, prop, rule, expected):
        self.outcomes.append((prop, 'failed', rule, expected))

    def passed(self):
        for prop, status, rule, expected in self.outcomes:
            if status != 'passed':
                continue
            yield prop.name, status, rule.__name__, getattr(expected, '__name__', expected)

    def failed(self):
        for prop, status, rule, expected in self.outcomes:
            if status != 'failed':
                continue
            yield prop.name, status, rule.__name__, getattr(expected, '__name__', expected)
