import pytest

from plastron.files import StringSource, FileSpec

OCTET_STREAM_SOURCE = StringSource(content='', mimetype='application/octet-stream')
HTML_SOURCE = StringSource(content='', mimetype='text/html')
TIFF_SOURCE = StringSource(content='', mimetype='image/tiff')


@pytest.mark.parametrize(
    ('spec_string', 'name', 'label', 'usage'),
    [
        ('foo.tif', 'foo.tif', None, None),
        ('<preservation>foo.tif', 'foo.tif', None, 'preservation'),
        ('Page 1:<preservation>foo.tif', 'foo.tif', 'Page 1', 'preservation'),
        ('Page 1:foo.tif', 'foo.tif', 'Page 1', None),
    ]
)
def test_parse_file_spec(spec_string, name, label, usage):
    file_spec = FileSpec.parse(spec_string)
    assert file_spec.name == name
    assert file_spec.label == label
    assert file_spec.usage == usage


@pytest.mark.parametrize(
    ('file_spec', 'expected_rootname'),
    [
        (FileSpec('foo.tif'), 'foo'),
        (FileSpec('foo.bar.tif'), 'foo.bar'),
        (FileSpec('foo'), 'foo'),
    ]
)
def test_file_spec_rootname(file_spec, expected_rootname):
    assert file_spec.rootname == expected_rootname


@pytest.mark.parametrize(
    ('file_spec', 'expected_string'),
    [
        (FileSpec('foo.tif'), 'foo.tif'),
        (FileSpec('foo.tif', label='Page 1'), 'Page 1:foo.tif'),
        (FileSpec('foo.tif', label='Page 1', usage='preservation'), 'Page 1:<preservation>foo.tif'),
        (FileSpec('foo.tif', usage='preservation'), '<preservation>foo.tif'),
    ]
)
def test_stringify_file_spec(file_spec, expected_string):
    assert str(file_spec) == expected_string
