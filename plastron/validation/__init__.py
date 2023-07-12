import logging
import re

from edtf_validate.valid_edtf import is_valid as is_valid_edtf
from iso639 import is_valid639_1, is_valid639_2

logger = logging.getLogger(__name__)


def validate(item):
    result = item.validate()

    if result.is_valid():
        logger.info(f'"{item}" passed metadata validation')
        for outcome in result.passed():
            logger.debug(f'  ✓ {outcome}')
    else:
        logger.warning(f'"{item}" failed metadata validation')
        for outcome in result.failed():
            logger.warning(f'  ✗ {outcome}')
        for outcome in result.passed():
            logger.debug(f'  ✓ {outcome}')

    return result


def is_edtf_formatted(value):
    # Allow blank values
    if str(value).strip() == "":
        return True
    return is_valid_edtf(str(value))


def is_valid_iso639_code(value):
    return is_valid639_1(value) or is_valid639_2(value)


def is_handle(value: str) -> bool:
    return bool(re.match('hdl:[^/]+/.*', value))


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


class ValidationError(Exception):
    pass
