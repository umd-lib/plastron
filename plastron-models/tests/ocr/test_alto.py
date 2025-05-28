import pytest
from lxml import etree

from plastron.ocr.core import XYWH, BBox
from plastron.ocr.alto import ALTOResource


@pytest.fixture
def alto(datadir):
    with (datadir / 'alto.xml').open() as fh:
        xmldoc = etree.parse(fh)
    return ALTOResource(xmldoc, (400, 400))


def test_alto_resource(alto):
    assert len(list(alto.blocks)) == 2
    assert alto.block('NON-EXISTENT IDENTIFIER') is None
    block = alto.block('P1_TB00003')
    assert block is not None
    assert block.id == 'P1_TB00003'

    assert block.xywh == XYWH(339, 780, 216, 44)
    assert block.bbox == BBox(339, 780, 555, 824)


def test_alto_block(alto):
    block = alto.block('P1_TB00006')
    assert str(block) == 'VARSITY BASKETERS\n'


def test_alto_line(alto):
    block = alto.block('P1_TB00006')
    line = next(block.lines())
    assert str(line) == 'VARSITY BASKETERS'


def test_alto_words(alto):
    block = alto.block('P1_TB00006')
    line = next(block.lines())
    assert set(str(w) for w in line.words()) == {'VARSITY', 'BASKETERS'}


def test_alto_all_words(alto):
    assert set(str(w) for w in alto.words()) == {'Vol.', 'VI', 'VARSITY', 'BASKETERS'}
