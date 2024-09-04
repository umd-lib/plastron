import re

from edtf_validate.valid_edtf import is_valid as is_valid_edtf
from iso639.language import Language, LanguageNotFoundError


def is_edtf_formatted(value):
    """an EDTF-formatted date"""
    # Allow blank values
    if str(value).strip() == "":
        return True
    return is_valid_edtf(str(value))


def is_valid_iso639_code(value):
    """a valid ISO-639 language code"""
    try:
        Language.match(value)
        return True
    except LanguageNotFoundError:
        return False


def is_handle(value: str) -> bool:
    """a handle URI"""
    return bool(re.match('hdl:[^/]+/.*', value))


def is_iso_8601_date(value: str) -> bool:
    """an ISO 8601 date string (YYYY-MM-DD)"""
    return bool(re.match(r'^\d\d\d\d-\d\d-\d\d$', value))
