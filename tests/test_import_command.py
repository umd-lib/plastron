import pytest
from plastron.commands import importcommand
from plastron.files import LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.models import Item, umdtype
from plastron.pcdm import Object
from plastron.rdf import RDFDataProperty
from plastron.stomp.messages import PlastronCommandMessage

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
    assert len(importcommand.build_file_groups(value)) == expected_count


def test_build_fields_with_default_datatype():
    fields = importcommand.build_fields(['Accession Number'], Item)
    assert fields['accession_number'][0]['datatype'] == umdtype.accessionNumber


def test_build_fields_without_default_datatype():
    fields = importcommand.build_fields(['Identifier'], Item)
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
    assert(len(list(importcommand.parse_value_string('', column, prop_type))) == 0)
    # single value
    assert(len(list(importcommand.parse_value_string('foo', column, prop_type))) == 1)
    # single value, followed by an empty string
    assert(len(list(importcommand.parse_value_string('foo|', column, prop_type))) == 1)
    # two values
    assert(len(list(importcommand.parse_value_string('foo|bar', column, prop_type))) == 2)
    # two values, with an empty string between
    assert(len(list(importcommand.parse_value_string('foo||bar', column, prop_type))) == 2)


# sample file group to use in add_files_* tests
ADD_FILES_GROUPS = importcommand.build_file_groups('foo.jpg;foo.tiff;bar.jpg;baz.pdf')


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


# "Flat" layout config
flat_repo_config = {
    'REST_ENDPOINT': 'http://example.com/rest',
    'RELPATH': '/pcdm',
    'LOG_DIR': 'logs',
    'STRUCTURE': 'flat'
}

# "Hierarchical" layout config
hierarchical_repo_config = {
    'REST_ENDPOINT': 'http://example.com/rest',
    'RELPATH': '/dc/2021/2',
    'LOG_DIR': 'logs',
    'STRUCTURE': 'hierarchical'
}

# Import message does not specify structure
no_structure_message = PlastronCommandMessage({
    'message-id': 'TEST-no-structure',
    'PlastronJobId': '1',
    'PlastronCommand': 'import',
})

# Import message specifies "flat" structure
flat_structure_message = PlastronCommandMessage({
    'message-id': 'TEST-flat-structure',
    'PlastronJobId': '1',
    'PlastronCommand': 'import',
    'PlastronArg-structure': 'flat'
})

# Import message specified "hierarchical" structure
hierarchical_structure_message = PlastronCommandMessage({
    'message-id': 'TEST-hierarchical-structure',
    'PlastronJobId': '1',
    'PlastronCommand': 'import',
    'PlastronArg-structure': 'hierarchical'
})


def test_repo_config_uses_structure_from_repo_config_if_no_structure_specified():
    # Flat structure in repo_config
    args = cmd.parse_message(no_structure_message)

    new_repo_config = cmd.repo_config(flat_repo_config, args)
    assert new_repo_config['STRUCTURE'] == 'flat'

    # Hierarchical structure in repo_config
    args = cmd.parse_message(no_structure_message)

    new_repo_config = cmd.repo_config(hierarchical_repo_config, args)
    assert new_repo_config['STRUCTURE'] == 'hierarchical'


def test_repo_config_uses_structure_from_message():
    # Hierarchical structure specified in message
    args = cmd.parse_message(hierarchical_structure_message)

    new_repo_config = cmd.repo_config(flat_repo_config, args)
    assert new_repo_config['STRUCTURE'] == 'hierarchical'

    # Flat structure specified in message
    args = cmd.parse_message(flat_structure_message)

    new_repo_config = cmd.repo_config(hierarchical_repo_config, args)
    assert new_repo_config['STRUCTURE'] == 'flat'


# "relpath" layout config
relpath_repo_config = {
    'REST_ENDPOINT': 'http://example.com/rest',
    'RELPATH': '/pcdm',
    'LOG_DIR': 'logs',
    'STRUCTURE': 'flat'
}

# Import message without relpath
no_relpath_message = PlastronCommandMessage({
    'message-id': 'TEST-without-relpath',
    'PlastronJobId': '1',
    'PlastronCommand': 'import',
    'PlastronArg-structure': 'flat'
})

relpath_message = PlastronCommandMessage({
    'message-id': 'TEST-with-relpath',
    'PlastronJobId': '1',
    'PlastronCommand': 'import',
    'PlastronArg-structure': 'flat',
    'PlastronArg-relpath': '/test-relpath'
})


def test_repo_config_uses_relpath_from_repo_config_if_no_relpath_specified():
    # Flat structure in repo_config
    args = cmd.parse_message(no_relpath_message)

    new_repo_config = cmd.repo_config(relpath_repo_config, args)
    assert new_repo_config['RELPATH'] == '/pcdm'


def test_repo_config_uses_relpath_from_message():
    # Hierarchical structure specified in message
    args = cmd.parse_message(relpath_message)

    new_repo_config = cmd.repo_config(flat_repo_config, args)
    assert new_repo_config['RELPATH'] == '/test-relpath'
