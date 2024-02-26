import pytest

from plastron.serializers.csv import build_lookup_index


@pytest.mark.parametrize(
    ('index_string', 'expected_index'),
    [
        (None, {}),
        ('', {}),
        ('creator[0]=#alpha', {'creator': {0: 'alpha'}}),
        (
            'creator[0]=#alpha;subject[0]=#beta;subject[1]=#gamma',
            {'creator': {0: 'alpha'}, 'subject': {0: 'beta', 1: 'gamma'}},
        ),
    ]
)
def test_build_lookup_index(index_string, expected_index):
    assert build_lookup_index(index_string) == expected_index
