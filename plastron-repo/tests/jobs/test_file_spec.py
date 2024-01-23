from plastron.files import StringSource
from plastron.jobs import FileSpec

OCTET_STREAM_SOURCE = StringSource(content='', mimetype='application/octet-stream')
HTML_SOURCE = StringSource(content='', mimetype='text/html')
TIFF_SOURCE = StringSource(content='', mimetype='image/tiff')


def test_file_spec_name():
    file_spec = FileSpec(name='foo.tif', source=TIFF_SOURCE)
    assert file_spec.name == 'foo.tif'
    assert str(file_spec) == 'foo.tif'
