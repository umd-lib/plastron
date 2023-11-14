import pytest

from plastron.serializers.csv import split_escaped


@pytest.mark.parametrize(
    ('string', 'separator', 'expected'),
    [
        ('foo', '|', ['foo']),
        ('foo|', '|', ['foo', '']),
        ('foo|bar|baz\\|flip', '|', ['foo', 'bar', 'baz|flip']),
        ('foo|bar|baz\\|flip;a;b\\;c', ';', ['foo|bar|baz|flip', 'a', 'b;c']),
        ('\\\\foo|bar|baz\\|flip;a;b\\;c', ';', ['\\foo|bar|baz|flip', 'a', 'b;c']),
        (None, '|', []),
        ('', '|', []),
    ]
)
def test_split_escaped(string, separator, expected):
    assert split_escaped(string, separator) == expected
