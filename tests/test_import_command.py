from importlib import import_module
from plastron.files import LocalFile, RemoteFile, ZipFile

imp = import_module('plastron.commands.import')


def test_build_file_groups():
    assert len(imp.build_file_groups('')) == 0
    assert len(imp.build_file_groups('foo.jpg;foo.png')) == 1
    assert len(imp.build_file_groups('foo.jpg;bar.jpg')) == 2


def test_get_source():
    assert isinstance(imp.get_source('zip:foo.zip', 'bar.jpg'), ZipFile)
    assert isinstance(imp.get_source('sftp://user@example.com/foo', 'bar.jpg'), RemoteFile)
    assert isinstance(imp.get_source('zip+sftp://user@example.com/foo.zip', 'bar.jpg'), ZipFile)
    assert isinstance(imp.get_source('/foo', 'bar'), LocalFile)
