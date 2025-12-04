import pytest
from lxml import etree

from plastron.ocr import HOCRResource
from plastron.ocr.core import BBox, XYWH, Scale
from plastron.ocr.hocr import Word, ContentArea


def test_hocr(datadir):
    with (datadir / 'sample.hocr').open() as fh:
        doc = etree.parse(fh)

    hocr = HOCRResource(doc, (400, 400))
    assert len(list(hocr.blocks)) == 5
    assert hocr.block('NON-EXISTENT') is None
    area: ContentArea = hocr.block('block_1_5')
    assert area is not None
    assert area.id == 'block_1_5'

    assert area.bbox == BBox(3443, 909, 4402, 1060)
    assert area.xywh == XYWH(3443, 909, 959, 151)


@pytest.mark.issue(key='LIBFCREPO-1743', url='https://umd-dit.atlassian.net/browse/LIBFCREPO-1743')
@pytest.mark.parametrize(
    ('xml', 'expected_content'),
    [
        ('<span class="ocrx_word" id="word_8" title="bbox 1366 1634 1472 1675">Trail</span>', 'Trail'),
        ('<span class="ocrx_word" id="word_8" title="bbox 1366 1634 1472 1675"><strong>Trail</strong></span>', 'Trail'),
        ('<span class="ocrx_word" id="word_8" title="bbox 1366 1634 1472 1675"><b><i>Trail</i></b></span>', 'Trail'),
        ('<span class="ocrx_word" id="word_8" title="bbox 1366 1634 1472 1675"><b>Trail</b>head</span>', 'Trailhead'),
        ('<span class="ocrx_word" id="word_8" title="bbox 1366 1634 1472 1675"></span>', ''),
    ]
)
def test_hocr_word_with_child_elements(xml, expected_content):
    word_element = etree.fromstring(xml)
    word = Word(word_element, Scale.from_resolution((400, 400)))
    assert word.content == expected_content
    assert str(word) == expected_content
