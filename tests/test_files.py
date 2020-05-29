from plastron.files import LocalFile, RemoteFile, ZipFile


def test_local_file():
    f = LocalFile('/foo/bar')
    assert f.localpath == '/foo/bar'


def test_remote_file():
    f = RemoteFile('sftp://user@example.com/foo/bar.jpg')
    assert f.sftp_uri.username == 'user'
    assert f.sftp_uri.hostname == 'example.com'
    assert f.sftp_uri.path == '/foo/bar.jpg'


def test_zip_file():
    f = ZipFile('foo.zip', 'bar.jpg')
    assert f.zip_filename == 'foo.zip'
    assert f.path == 'bar.jpg'


def test_remote_zip_file():
    f = ZipFile('sftp://user@example.com/foo.zip', 'bar.jpg')
    assert f.zip_sftp_uri.username == 'user'
    assert f.zip_sftp_uri.hostname == 'example.com'
    assert f.zip_sftp_uri.path == '/foo.zip'
    assert f.path == 'bar.jpg'
