import pytest

from plastron.client import TypedText


@pytest.mark.parametrize(
    ('media_type', 'value', 'expected_bool', 'expected_len'),
    [
        ('text/plain', 'foobar', True, 6),
        ('application/xml', '<x>123</x>', True, 10),
        ('text/plain', '', False, 0),
    ]
)
def test_typed_text(media_type, value, expected_bool, expected_len):
    text = TypedText(media_type, value)
    assert text.media_type == media_type
    assert text.value == value
    assert str(text) == value
    assert bool(text) == expected_bool
    assert len(text) == expected_len
