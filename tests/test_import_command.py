import copy
from importlib import import_module
from plastron.files import LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.http import Repository, FlatCreator, HierarchicalCreator
from plastron.pcdm import Object
from plastron.rdf import RDFDataProperty
from plastron.stomp.messages import PlastronCommandMessage

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


def test_override_repo_config_uses_structure_from_repo_config_if_no_structure_specified():
    # Flat structure in repo_config
    args = cmd.parse_message(no_structure_message)

    new_repo_config = cmd.override_repo_config(flat_repo_config, args)
    assert new_repo_config['STRUCTURE'] == 'flat'

    # Hierarchical structure in repo_config
    args = cmd.parse_message(no_structure_message)

    new_repo_config = cmd.override_repo_config(hierarchical_repo_config, args)
    assert new_repo_config['STRUCTURE'] == 'hierarchical'


def test_override_repo_config_uses_structure_from_message():
    # Hierarchical structure specified in message
    args = cmd.parse_message(hierarchical_structure_message)

    new_repo_config = cmd.override_repo_config(flat_repo_config, args)
    assert new_repo_config['STRUCTURE'] == 'hierarchical'

    # Flat structure specified in message
    args = cmd.parse_message(flat_structure_message)

    new_repo_config = cmd.override_repo_config(hierarchical_repo_config, args)
    assert new_repo_config['STRUCTURE'] == 'flat'
