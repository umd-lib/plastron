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
