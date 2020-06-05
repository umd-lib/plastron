from importlib import import_module
from plastron.files import LocalFile, RemoteFile, ZipFile

imp = import_module('plastron.commands.import')
cmd = imp.Command()


def test_build_file_groups():
    assert len(imp.build_file_groups('')) == 0
    assert len(imp.build_file_groups('foo.jpg;foo.png')) == 1
    assert len(imp.build_file_groups('foo.jpg;bar.jpg')) == 2


def test_get_source():
    assert isinstance(cmd.get_source('zip:foo.zip', 'bar.jpg'), ZipFile)
    assert isinstance(cmd.get_source('sftp://user@example.com/foo', 'bar.jpg'), RemoteFile)
    assert isinstance(cmd.get_source('zip+sftp://user@example.com/foo.zip', 'bar.jpg'), ZipFile)
    assert isinstance(cmd.get_source('/foo', 'bar'), LocalFile)
