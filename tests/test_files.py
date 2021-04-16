from unittest.mock import MagicMock
from tempfile import TemporaryFile
from uuid import uuid4
from zipfile import ZipFile

from plastron.files import HTTPFileSource, LocalFileSource, RemoteFileSource, ZipFileSource


def test_local_file():
    f = LocalFileSource('/foo/bar')
    assert f.localpath == '/foo/bar'


def test_remote_file():
    f = RemoteFileSource('sftp://user@example.com/foo/bar.jpg')
    assert f.sftp_uri.username == 'user'
    assert f.sftp_uri.hostname == 'example.com'
    assert f.sftp_uri.path == '/foo/bar.jpg'


def test_zip_file():
    f = ZipFileSource('foo.zip', 'bar.jpg')
    assert f.source.localpath == 'foo.zip'
    assert f.source.filename == 'bar.jpg'


def test_remote_zip_file():
    f = ZipFileSource('sftp://user@example.com/foo.zip', 'bar.jpg')
    assert f.source.sftp_uri.username == 'user'
    assert f.source.sftp_uri.hostname == 'example.com'
    assert f.source.sftp_uri.path == '/foo.zip'
    assert f.path == 'bar.jpg'


def test_http_file():
    f = HTTPFileSource('http://example.com/test.jpg')
    assert f.uri == 'http://example.com/test.jpg'
    assert f.filename == 'test.jpg'


def test_nonexistent_local_file_source():
    # pick a random filename string that is unlikely to exist
    f = LocalFileSource(str(uuid4()))
    assert f.exists() is False


def test_nonexistent_zip_file_source():
    # create an empty zip file
    with TemporaryFile() as tmp_file:
        zip_file = ZipFile(tmp_file, mode='w')
        f = ZipFileSource(zip_file, 'foo.jpg')
        assert f.exists() is False


def test_nonexistent_http_file_source():
    # pick a random filename string that is unlikely to exist
    uri = f'http://www.example.com/{uuid4()}'
    f = HTTPFileSource(uri)
    assert f.exists() is False


def test_zip_file_source_exists():
    zip_file_source = ZipFileSource('tests/resources/sample.zip', 'sample_image.jpg')
    assert zip_file_source.exists()


def setup_remote_file_source_mock(remote_file_source):
    # Wrap the given remote_file_sourcee in a mock, so that the methods that
    # are accessed can be queried
    mock = MagicMock(wraps=remote_file_source)
    # Return a "real" Zip, so we don't have to mock zipfile.ZipFile
    mock.open.return_value = open('tests/resources/sample.zip', 'rb')
    # Need to mock __exit__ (following code in BinarySource.__exit__) because
    # "wraps" doesn't handle magic methods
    mock.__exit__.side_effect = (lambda _arg1, _arg2, _arg3: mock.close())

    return mock


def test_zip_file_source_exists_closes_remote_file_source():
    zip_file_source = ZipFileSource('sftp://user@example.com/sample.zip', 'sample_image.jpg')
    remote_file_source = zip_file_source.source

    remote_file_source_mock = setup_remote_file_source_mock(remote_file_source)
    zip_file_source.source = remote_file_source_mock

    assert zip_file_source.exists()
    remote_file_source_mock.open.assert_called()
    remote_file_source_mock.close.assert_called()


def test_zip_file_source_exists_closes_remote_file_source_when_file_does_not_exist():
    zip_file_source = ZipFileSource('sftp://user@example.com/sample.zip', 'does_not_exist.jpg')
    remote_file_source = zip_file_source.source

    remote_file_source_mock = setup_remote_file_source_mock(remote_file_source)
    zip_file_source.source = remote_file_source_mock

    assert not zip_file_source.exists()
    remote_file_source_mock.open.assert_called()
    remote_file_source_mock.close.assert_called()
