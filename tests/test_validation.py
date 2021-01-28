import pytest
from plastron.validation import is_edtf_formatted
from plastron.validation.rules import required


def test_required():
    # no values fails
    assert required([]) is False
    # no values but not actually required passes
    assert required([], False) is True
    # empty string fails
    assert required(['']) is False
    # blank string fails
    assert required(['  ']) is False
    # non-empty strings pass
    assert required(['foo']) is True
    assert required(['0']) is True
    # non-string values pass
    assert required([0]) is True
    assert required([1.0]) is True
    # only need one non-empty string to pass
    assert required(['foo', '']) is True


@pytest.mark.parametrize(
    'datetime_string', [
        # dates at 11pm fail in edtf 4.0.1
        # these pass when using edtf-validate 1.1.0
        '2020-07-10T23:44:38Z',
        '2020-07-10T23:15:47Z',
        '2020-07-20T23:52:29Z',
        '2020-07-24T23:46:17Z',
        # same dates, but at 10pm, pass
        '2020-07-10T22:44:38Z',
        '2020-07-10T22:15:47Z',
        '2020-07-20T22:52:29Z',
        '2020-07-24T22:46:17Z',
    ])
def test_is_edtf_formatted(datetime_string):
    assert is_edtf_formatted(datetime_string) is True
