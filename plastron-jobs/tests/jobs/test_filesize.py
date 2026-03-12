import pytest

from plastron.jobs.exportjob import FileSize


@pytest.mark.parametrize(
    ('size', 'expected'),
    [
        (1024, '1.0 KB'),
        (1024 ** 2, '1.0 MB'),
        (1024 ** 3, '1.0 GB'),
        (1024 ** 4, '1.0 TB'),
    ]
)
def test_filesize(size, expected):
    assert str(FileSize(size)) == expected
