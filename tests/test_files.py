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
