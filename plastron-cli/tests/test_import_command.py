import pytest

import plastron.jobs.utils
from plastron.cli.commands import importcommand
from plastron.files import LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.jobs import build_fields
from plastron.models import Item, umdtype
from plastron.rdf.pcdm import Object
from plastron.rdf.rdf import RDFDataProperty

cmd = importcommand.Command()


@pytest.mark.parametrize(
    ('value', 'expected_count'),
    [
        ('', 0),
        ('foo.jpg;foo.png', 1),
        ('foo.jpg;bar.jpg', 2)
    ]
)
def test_build_file_groups(value, expected_count):
    assert len(plastron.jobs.utils.build_file_groups(value)) == expected_count


def test_build_fields_with_default_datatype():
    fields = build_fields(['Accession Number'], Item)
    assert fields['accession_number'][0]['datatype'] == umdtype.accessionNumber


def test_build_fields_without_default_datatype():
    fields = build_fields(['Identifier'], Item)
    assert fields['identifier'][0]['datatype'] is None


@pytest.mark.parametrize(
    ('base_location', 'path', 'expected_class'),
    [
        ('zip:foo.zip', 'bar.jpg', ZipFileSource),
        ('sftp://user@example.com/foo', 'bar.jpg', RemoteFileSource),
        ('zip+sftp://user@example.com/foo.zip', 'bar.jpg', ZipFileSource),
        ('/foo', 'bar', LocalFileSource)
    ]
)
def test_get_source(base_location, path, expected_class):
    assert isinstance(cmd.get_source(base_location, path), expected_class)


def test_parse_value_string():
    column = {'lang_code': None, 'datatype': None}
    prop_type = type('test', (RDFDataProperty,), {'datatype': None})

    # the empty string should parse to the empty list
    assert len(list(plastron.jobs.utils.parse_value_string('', column, prop_type))) == 0
    # single value
    assert len(list(plastron.jobs.utils.parse_value_string('foo', column, prop_type))) == 1
    # single value, followed by an empty string
    assert len(list(plastron.jobs.utils.parse_value_string('foo|', column, prop_type))) == 1
    # two values
    assert len(list(plastron.jobs.utils.parse_value_string('foo|bar', column, prop_type))) == 2
    # two values, with an empty string between
    assert len(list(plastron.jobs.utils.parse_value_string('foo||bar', column, prop_type))) == 2


# sample file group to use in add_files_* tests
ADD_FILES_GROUPS = plastron.jobs.utils.build_file_groups('foo.jpg;foo.tiff;bar.jpg;baz.pdf')


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
