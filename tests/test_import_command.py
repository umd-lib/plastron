from importlib import import_module
from plastron.files import LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.pcdm import Object
from plastron.rdf import RDFDataProperty

imp = import_module('plastron.commands.import')
cmd = imp.Command()


def test_build_file_groups():
    assert len(imp.build_file_groups('')) == 0
    assert len(imp.build_file_groups('foo.jpg;foo.png')) == 1
    assert len(imp.build_file_groups('foo.jpg;bar.jpg')) == 2


def test_get_source():
    assert isinstance(cmd.get_source('zip:foo.zip', 'bar.jpg'), ZipFileSource)
    assert isinstance(cmd.get_source('sftp://user@example.com/foo', 'bar.jpg'), RemoteFileSource)
    assert isinstance(cmd.get_source('zip+sftp://user@example.com/foo.zip', 'bar.jpg'), ZipFileSource)
    assert isinstance(cmd.get_source('/foo', 'bar'), LocalFileSource)


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


# sample file group to use in add_files_* tests
ADD_FILES_GROUPS = imp.build_file_groups('foo.jpg;foo.tiff;bar.jpg;baz.pdf')


def test_add_files_paged():
    item = Object()
    files_added = cmd.add_files(item, ADD_FILES_GROUPS, base_location='/somewhere', create_pages=True)
    # all 4 files should be added
    assert files_added == 4
    # 3 pages should be added
    assert len(item.members) == 3
    # 3 proxies should be added
    assert len(item.proxies()) == 3
    # no files should be directly added to the item
    assert len(item.files) == 0
    # page 1 (foo) should have 2 files
    assert len(item.members[0].files) == 2
    # page 2 (bar) should have 1 file
    assert len(item.members[1].files) == 1
    # page 3 (baz) should have 1 file
    assert len(item.members[2].files) == 1


def test_add_files_unpaged():
    item = Object()
    files_added = cmd.add_files(item, ADD_FILES_GROUPS, base_location='/somewhere', create_pages=False)
    # all 4 files should be added
    assert files_added == 4
    # no pages should be added
    assert len(item.members) == 0
    # no page proxies should be added
    assert len(item.proxies()) == 0
    # all 4 files should be attached directly to the item
    assert len(item.files) == 4
