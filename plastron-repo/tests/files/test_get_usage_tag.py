import pytest

from plastron.files import get_usage_tag
from plastron.models.pcdm import PCDMFile
from plastron.namespaces import pcdmuse, fabio, dcmitype


@pytest.mark.parametrize(
    ('file_obj', 'expected_tag'),
    [
        (PCDMFile(rdf_type=pcdmuse.PreservationMasterFile), 'preservation'),
        (PCDMFile(rdf_type=pcdmuse.ExtractedText), 'ocr'),
        (PCDMFile(rdf_type=fabio.MetadataFile), 'metadata'),
        (PCDMFile(), None),
        (PCDMFile(rdf_type=dcmitype.NotARealType), None),
    ]
)
def test_get_usage_tag(file_obj, expected_tag):
    assert get_usage_tag(file_obj) == expected_tag
