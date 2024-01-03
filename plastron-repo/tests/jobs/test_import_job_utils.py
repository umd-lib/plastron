import pytest
from rdflib import URIRef

from plastron.files import LocalFileSource, RemoteFileSource, ZipFileSource
from plastron.jobs.imports import ImportJob
from plastron.jobs.imports.spreadsheet import ColumnSpec, build_fields, build_file_groups, parse_value_string
from plastron.models import Item
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
    job = ImportJob(job_id='foo', jobs_dir=datadir)
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
