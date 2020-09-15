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
