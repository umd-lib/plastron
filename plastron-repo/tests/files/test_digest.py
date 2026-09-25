from http import HTTPStatus

import httpretty
import pytest

from plastron.files import StringSource, UnsupportedHashAlgorithm, LocalFileSource, ZipFileSource, HTTPFileSource

# these are the expected checksums for the "test_digest/turtle.jpg" file
# for the various hash algorithms
EXPECTED_DIGESTS = [
    ('sha1', '03412c19aa5c2393a34d39b392ecce6f86997b0a'),
    ('sha', '03412c19aa5c2393a34d39b392ecce6f86997b0a'),
    ('sha256', '172d837327d55e9df364dc0ff474b7fe526201b9c7f9c4e4919663338e747b18'),
    ('sha-256', '172d837327d55e9df364dc0ff474b7fe526201b9c7f9c4e4919663338e747b18'),
    (
        'sha-512',
        (
            '27c577ecfeb2ee2650937543cee4534053d8d371cdd9fa608e21d736abee42b5'
            '54b8a4944c7a65003c9c6d321ebcef9c9596581f3c5e6f9764594bb35b045431'
        ),
    ),
    ('sha-512/256', '2d39ed62d2a46d2a69d24412ed7ba21c5319857b6271662e5c7a162e5e3ea8cc'),
    ('md5', 'c90eb997fa55f496e321da4a1f07e165'),
]


@pytest.fixture
def string_source():
    return StringSource('foobar')


@pytest.fixture
def file_source(datadir):
    # original file source: https://commons.wikimedia.org/wiki/File:Green_Sea_Turtle_(43781715354).jpg
    return LocalFileSource(datadir / 'turtle.jpg')


@pytest.fixture
def zip_source(datadir):
    # original file source: https://commons.wikimedia.org/wiki/File:Green_Sea_Turtle_(43781715354).jpg
    return ZipFileSource(datadir / 'source.zip', 'turtle.jpg')


@pytest.fixture
def http_url():
    return 'http://example.com/turtle.jpg'


@pytest.fixture
def http_source(http_url: str):
    return HTTPFileSource(http_url)


@pytest.mark.parametrize(
    ('algorithm', 'expected_digest'),
    [
        ('sha1', '8843d7f92416211de9ebb963ff4ce28125932878'),
        ('sha', '8843d7f92416211de9ebb963ff4ce28125932878'),
        ('sha256', 'c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2'),
        ('sha-256', 'c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2'),
        (
            'sha-512',
            (
                '0a50261ebd1a390fed2bf326f2673c145582a6342d523204973d0219337f8161'
                '6a8069b012587cf5635f6925f1b56c360230c19b273500ee013e030601bf2425'
            ),
        ),
        ('sha-512/256', 'd014c752bc2be868e16330f47e0c316a5967bcbc9c286a457761d7055b9214ce'),
        ('md5', '3858f62230ac3c915f300c664312c63f'),
    ],
)
def test_string_source_digest(string_source, algorithm, expected_digest):
    digest = string_source.digest(algorithm)
    assert digest == f'{algorithm}={expected_digest}'


def test_default_digest(string_source):
    digest = string_source.digest()
    assert digest == 'sha1=8843d7f92416211de9ebb963ff4ce28125932878'


def test_unsupported_hash_algorithm(string_source):
    with pytest.raises(UnsupportedHashAlgorithm) as excinfo:
        string_source.digest('NOT_REAL')
    assert str(excinfo.value).startswith('Unsupported hash algorithm')


@pytest.mark.parametrize(
    ('algorithm', 'expected_digest'),
    EXPECTED_DIGESTS,
)
def test_local_file_source_digest(file_source, algorithm, expected_digest):
    digest = file_source.digest(algorithm)
    assert digest == f'{algorithm}={expected_digest}'


@pytest.mark.parametrize(
    ('algorithm', 'expected_digest'),
    EXPECTED_DIGESTS,
)
def test_zip_file_source_digest(zip_source, algorithm, expected_digest):
    digest = zip_source.digest(algorithm)
    assert digest == f'{algorithm}={expected_digest}'


@pytest.mark.parametrize(
    ('algorithm', 'expected_digest'),
    EXPECTED_DIGESTS,
)
@httpretty.activate
def test_http_file_source_digest(datadir, http_source, algorithm, expected_digest):
    httpretty.register_uri(
        method=httpretty.HEAD,
        uri=http_source.uri,
        status=HTTPStatus.OK,
        adding_headers={
            'Content-Type': 'image/jpeg',
        },
    )
    httpretty.register_uri(
        method=httpretty.GET,
        uri=http_source.uri,
        status=HTTPStatus.OK,
        adding_headers={
            'Content-Type': 'image/jpeg',
        },
        body=(datadir / 'turtle.jpg').read_bytes(),
    )
    digest = http_source.digest(algorithm)
    assert digest == f'{algorithm}={expected_digest}'
