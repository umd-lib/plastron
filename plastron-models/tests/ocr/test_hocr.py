from lxml import etree

from plastron.ocr import HOCRResource
from plastron.ocr.core import BBox, XYWH


def test_hocr(datadir):
    with (datadir / 'sample.hocr').open() as fh:
        doc = etree.parse(fh)

    hocr = HOCRResource(doc, (400, 400))
    assert len(list(hocr.blocks)) == 5
    assert hocr.block('NON-EXISTENT') is None
    area = hocr.block('block_1_5')
    assert area is not None
    assert area.id == 'block_1_5'

    assert area.bbox == BBox(3443, 909, 4402, 1060)
    assert area.xywh == XYWH(3443, 909, 959, 151)
