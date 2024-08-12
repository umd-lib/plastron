import re

from edtf_validate.valid_edtf import is_valid as is_valid_edtf
from iso639 import is_valid639_1, is_valid639_2


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
