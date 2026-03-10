from plastron.context import PlastronContext
from plastron.jobs.exportjob import ExportJob


def test_exportjob_without_binaries():
    job = ExportJob(
        context=PlastronContext(),
        export_format='csv',
        export_binaries=False,
        binary_types='',
        output_dest='foo.zip',
        uri_template='http://example.com/{id}',
        uris=[],
        key='',
    )
    assert job.mime_type_filter is None


def test_exportjob_with_binaries():
    job = ExportJob(
        context=PlastronContext(),
        export_format='csv',
        export_binaries=True,
        binary_types='image/tiff',
        output_dest='foo.zip',
        uri_template='http://example.com/{id}',
        uris=[],
        key='',
    )
    assert callable(job.mime_type_filter)
