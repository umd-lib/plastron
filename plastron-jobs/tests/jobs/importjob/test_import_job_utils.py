import pytest
from plastron.files import LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.jobs.importjob import ImportJob
from plastron.jobs.importjob.spreadsheet import (
    MetadataError,
    build_fields,
    build_file_groups,
)
from plastron.models.umd import Item

from plastron.namespaces import umdtype


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


@pytest.mark.parametrize(
    ('value', 'expected_count', 'expected_usages'),
    [
        ('foo.tif;foo.xml', 1, {'foo': {'foo.tif': None, 'foo.xml': None}}),
        ('<Preservation>foo.tif;<OCR>foo.xml', 1, {'foo': {'foo.tif': 'Preservation', 'foo.xml': 'OCR'}}),
        (
            '<Preservation>foo.tif;<OCR>foo.xml;bar.jpg;bar.xml',
            2,
            {'foo': {'foo.tif': 'Preservation', 'foo.xml': 'OCR'}, 'bar': {'bar.jpg': None, 'bar.xml': None}},
        ),
        ('Page 1:<Preservation>foo.tif;<OCR>foo.xml', 1, {'foo': {'foo.tif': 'Preservation', 'foo.xml': 'OCR'}}),
        ('<ocr>0004.xml;<ocr>0004.hocr', 1, {'0004': {'0004.xml': 'ocr', '0004.hocr': 'ocr'}}),
    ]
)
def test_build_file_groups_with_usage(value, expected_count, expected_usages):
    groups = build_file_groups(value)
    assert len(groups) == expected_count
    for rootname, usage_map in expected_usages.items():
        for name, usage in usage_map.items():
            assert groups[rootname].file(name).usage == usage


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


def test_build_file_groups_none_strategy():
    """Test the 'none' grouping strategy where each file is its own group"""
    value = 'foo.jpg;foo.png;bar.jpg'
    groups = build_file_groups(value, grouping_strategy='none')

    # Each file should be its own group
    assert len(groups) == 3

    # Check that we have one file per group
    assert all(len(group.files) == 1 for group in groups.values())

    # Check labels are auto-generated
    assert groups['foo.jpg'].label == 'Page 1'
    assert groups['foo.png'].label == 'Page 2'
    assert groups['bar.jpg'].label == 'Page 3'


def test_build_file_groups_none_strategy_with_labels():
    """Test the 'none' grouping strategy with explicit labels"""
    value = 'Front:foo.jpg;Back:foo.png;Side:bar.jpg'
    groups = build_file_groups(value, grouping_strategy='none')

    # Each file should be its own group
    assert len(groups) == 3

    # Check that labels are preserved
    assert groups['foo.jpg'].label == 'Front'
    assert groups['foo.png'].label == 'Back'
    assert groups['bar.jpg'].label == 'Side'


def test_build_file_groups_rootname_strategy():
    """Test the 'rootname' grouping strategy (default behavior)"""
    value = 'foo.jpg;foo.png;bar.jpg;bar.png'
    groups = build_file_groups(value, grouping_strategy='rootname')

    # Should have 2 groups: 'foo' and 'bar'
    assert len(groups) == 2

    # Each group should have 2 files
    assert len(groups['foo'].files) == 2
    assert len(groups['bar'].files) == 2

    # Labels should be auto-generated
    assert groups['foo'].label == 'Page 1'
    assert groups['bar'].label == 'Page 2'


def test_build_file_groups_rootname_strategy_explicit_labels():
    """Test the 'rootname' grouping strategy with explicit labels"""
    value = 'Cover:foo.jpg;foo.png;Back:bar.jpg;bar.png'
    groups = build_file_groups(value, grouping_strategy='rootname')

    assert len(groups) == 2

    assert groups['foo'].label == 'Cover'
    assert groups['bar'].label == 'Back'


def test_build_file_groups_pool_reports_none_strategy():
    """Test the use case described in the issue: pool reports with attachments"""
    # Simulating the pool reports use case where we have:
    # - Two attachments with the same base name but different extensions
    # - Another pair with the same base name but different extensions
    value = 'report1.pdf;report1.docx;report2.pdf;report2.docx'

    # Using 'rootname' (default) would group them, creating 2 pages
    groups_rootname = build_file_groups(value, grouping_strategy='rootname')
    assert len(groups_rootname) == 2

    # Using 'none' would create 4 separate attachments
    groups_none = build_file_groups(value, grouping_strategy='none')
    assert len(groups_none) == 4

    # Each file is its own group
    for group in groups_none.values():
        assert len(group.files) == 1


def test_build_file_groups_invalid_strategy():
    """Test that invalid grouping strategies raise ValueError"""
    with pytest.raises(ValueError) as e:
        build_file_groups('foo.jpg', grouping_strategy='invalid')

    assert 'Invalid grouping_strategy' in str(e.value)

