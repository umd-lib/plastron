from importlib import import_module
from plastron.files import LocalFile, RemoteFile, ZipFile
from plastron.rdf import RDFDataProperty

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


def test_parse_value_string():
    column = {'lang_code': None, 'datatype': None}
    prop_type = type('test', (RDFDataProperty,), {'datatype': None})

    # the empty string should parse to the empty list
    assert(len(list(imp.parse_value_string('', column, prop_type))) == 0)
    # single value
    assert(len(list(imp.parse_value_string('foo', column, prop_type))) == 1)
    # single value, followed by an empty string
    assert(len(list(imp.parse_value_string('foo|', column, prop_type))) == 1)
    # two values
    assert(len(list(imp.parse_value_string('foo|bar', column, prop_type))) == 2)
    # two values, with an empty string between
    assert(len(list(imp.parse_value_string('foo||bar', column, prop_type))) == 2)
