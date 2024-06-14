import re

from edtf_validate.valid_edtf import is_valid as is_valid_edtf
from iso639 import is_valid639_1, is_valid639_2

from plastron.validation.vocabularies import get_subjects


def is_edtf_formatted(value):
    """an EDTF-formatted date"""
    # Allow blank values
    if str(value).strip() == "":
        return True
    return is_valid_edtf(str(value))


def is_valid_iso639_code(value):
    """a valid ISO-639 language code"""
    return is_valid639_1(value) or is_valid639_2(value)


def is_handle(value: str) -> bool:
    """a handle URI"""
    return bool(re.match('hdl:[^/]+/.*', value))


def is_iso_8601_date(value: str) -> bool:
    """an ISO 8601 date string (YYYY-MM-DD)"""
    return bool(re.match(r'^\d\d\d\d-\d\d-\d\d$', value))


def is_from_vocabulary(vocab_uri):
    def _value_from_vocab(value):
        return value in get_subjects(vocab_uri)

    _value_from_vocab.__doc__ = f'from vocabulary {vocab_uri}'
    return _value_from_vocab
