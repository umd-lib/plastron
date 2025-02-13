import pytest
from rdflib import URIRef

from plastron.files import LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.jobs.importjob import ImportJob
from plastron.jobs.importjob.spreadsheet import ColumnSpec, build_fields, build_file_groups, parse_value_string, \
    MetadataError
from plastron.models.umd import Item
from plastron.namespaces import umdtype
from plastron.rdfmapping.descriptors import DataProperty


@pytest.mark.parametrize(
    ('value', 'expected_count'),
    [
        ('', 0),
        ('foo.jpg;foo.png', 1),
        ('foo.jpg;bar.jpg', 2)
    ]
)
def test_build_file_groups(value, expected_count):
    assert len(build_file_groups(value)) == expected_count


@pytest.mark.parametrize(
    ('value', 'error_message'),
    [
        # mismatched labels
        ('Page 1:foo.jpg;p 1:foo.png', 'Multiple files with rootname "foo" have differing labels'),
        # missing labels
        ('Page 1:foo.jpg;bar.jpg', 'If any file group has a label, all file groups must have a label'),
    ]
)
def test_build_file_groups_errors(value, error_message):
    with pytest.raises(MetadataError) as e:
        build_file_groups(value)
    assert str(e.value) == error_message


@pytest.mark.parametrize(
    ('value', 'expected_count', 'expected_labels'),
    [
        ('Page 1:foo.jpg;foo.png;Page 2:bar.jpg;bar.png', 2, {'foo': 'Page 1', 'bar': 'Page 2'}),
        ('foo.jpg;Page 1:foo.png;Page 2:bar.jpg;bar.png', 2, {'foo': 'Page 1', 'bar': 'Page 2'}),
        ('p. 37:foo.jpg;foo.png;p. 38:bar.jpg;bar.png', 2, {'foo': 'p. 37', 'bar': 'p. 38'}),
        ('XVII:foo.jpg;foo.png;XV:bar.jpg;bar.png', 2, {'foo': 'XVII', 'bar': 'XV'}),
        ('Page 1:foo.jpg;foo.png', 1, {'foo': 'Page 1'}),
        # default labels
        ('foo.jpg;foo.png;bar.jpg;bar.png;baz.jpg', 3, {'foo': 'Page 1', 'bar': 'Page 2', 'baz': 'Page 3'}),
    ]
)
def test_build_file_groups_labeled(value, expected_count, expected_labels):
    groups = build_file_groups(value)
    assert len(groups) == expected_count
    for rootname, label in expected_labels.items():
        assert groups[rootname].label == label


def test_build_fields_with_default_datatype():
    fields = build_fields(['Accession Number'], Item)
    assert fields['accession_number'][0].datatype == umdtype.accessionNumber


def test_build_fields_without_default_datatype():
    fields = build_fields(['Identifier'], Item)
    assert fields['identifier'][0].datatype is None


@pytest.mark.parametrize(
    ('base_location', 'path', 'expected_class'),
    [
        ('zip:foo.zip', 'bar.jpg', ZipFileSource),
        ('sftp://user@example.com/foo', 'bar.jpg', RemoteFileSource),
        ('zip+sftp://user@example.com/foo.zip', 'bar.jpg', ZipFileSource),
        ('/foo', 'bar', LocalFileSource)
    ]
)
def test_get_source(datadir, base_location, path, expected_class):
    job = ImportJob(job_id='foo', job_dir=datadir)
    assert isinstance(job.get_source(base_location, path), expected_class)


@pytest.mark.parametrize(
    ('input_string', 'expected_count'),
    [
        # the empty string should parse to the empty list
        ('', 0),
        # single value
        ('foo', 1),
        # single value, followed by an empty string
        ('foo|', 1),
        # two values
        ('foo|bar', 2),
        # two values, with an empty string between
        ('foo||bar', 2),

    ]
)
def test_parse_value_string(input_string, expected_count):
    prop = DataProperty(predicate=URIRef('http://example.com/test'), datatype=None)
    column_spec = ColumnSpec(attrs='test', header='Test', prop=prop, lang_code=None, datatype=None)
    assert len(list(parse_value_string(input_string, column_spec))) == expected_count
