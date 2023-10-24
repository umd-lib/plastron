import pytest

from plastron.serializers.csv import ColumnHeader


@pytest.mark.parametrize(
    ('label', 'language', 'expected_string'),
    [
        ('Foo', None, 'Foo'),
        ('Bar', 'en', 'Bar [en]'),
    ]
)
def test_column_header(label, language, expected_string):
    header = ColumnHeader(label=label, language=language)
    assert header.label == label
    assert header.language == language
    assert str(header) == expected_string


@pytest.mark.parametrize(
    ('input_string', 'label', 'language'),
    [
        ('Foo', 'Foo', None),
        ('Bar [en]', 'Bar', 'en'),
    ]
)
def test_column_header_from_string(input_string, label, language):
    header = ColumnHeader.from_string(input_string)
    assert header.label == label
    assert header.language == language
