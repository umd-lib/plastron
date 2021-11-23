import pytest

from plastron.commands.importcommand import split_escaped


@pytest.mark.parametrize(
    ('string', 'separator', 'expected'),
    [
        ('foo', '|', ['foo']),
        ('foo|', '|', ['foo', '']),
        ('foo|bar|baz\\|flip', '|', ['foo', 'bar', 'baz|flip']),
        ('foo|bar|baz\\|flip;a;b\\;c', ';', ['foo|bar|baz|flip', 'a', 'b;c']),
        ('\\\\foo|bar|baz\\|flip;a;b\\;c', ';', ['\\foo|bar|baz|flip', 'a', 'b;c']),
    ]
)
def test_split_escaped(string, separator, expected):
    assert split_escaped(string, separator) == expected
