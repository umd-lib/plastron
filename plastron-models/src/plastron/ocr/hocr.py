"""[hOCR](https://kba.github.io/hocr-spec/1.2/) OCR classes"""

import re
from typing import Iterable

# noinspection PyProtectedMember
from lxml.etree import _ElementTree, _Element

from plastron.ocr.core import XYWH, BBox, Scale, RegionBase, OCRResource, BlockRegion, LineRegion, WordRegion

XMLNS = {'html': 'http://www.w3.org/1999/xhtml'}


class HOCRResource(OCRResource):
    """hOCR OCR HTML resource"""
    def __init__(self, doc: _ElementTree, image_resolution: tuple[int, int]):
        self.doc = doc
        capabilities_element = self.doc.xpath('//html:meta[@name="ocr-capabilities"]', namespaces=XMLNS)[0]
        self.capabilities = capabilities_element.get('content').split(' ')
        self.scale = Scale.from_resolution(image_resolution, 'pixel')

    def get_block_nodes(self) -> Iterable[_Element]:
        """Get the `.ocr_carea` descendant elements of this resource."""
        return self.doc.xpath('//html:*[@class="ocr_carea"]', namespaces=XMLNS)

    def get_block_node(self, identifier: str) -> _Element:
        """Get the `.ocr_carea` descendant element whose `@id` matches
        the given `identifier`."""
        return self.doc.xpath("//html:*[@class='ocr_carea'][@id=$id]", id=identifier, namespaces=XMLNS)[0]

    def get_block(self, node: _Element) -> 'ContentArea':
        """Get the `ContentArea` object wrapping the given `.ocr_carea` element."""
        return ContentArea(node, self.scale)


class HOCRRegion(RegionBase):
    """Region within an `HOCRResource`."""
    def __init__(self, element: _Element, scale: Scale):
        self.element = element
        self.scale = scale
        self.id = self.element.get('id')
        properties = self.element.get('title')
        match = re.match(r'bbox (\d+) (\d+) (\d+) (\d+)', properties)
        self._bbox = BBox(*(int(c) for c in match.groups())).scale(self.scale)
        self._xywh = XYWH.from_bbox(self._bbox)


class ContentArea(HOCRRegion, BlockRegion):
    """Wraps an hOCR `.ocr_carea` element."""

    def get_line_nodes(self) -> Iterable[_Element]:
        """Get the `.ocr_line` child elements of this area."""
        return self.element.xpath('.//html:*[@class="ocr_line"]', namespaces=XMLNS)

    def get_line(self, node: _Element) -> 'Line':
        """Get a `Line` object wrapping the given `.ocr_line` element."""
        return Line(node, self.scale)


class Line(HOCRRegion, LineRegion):
    """Wraps an hOCR `.ocr_line` element."""

    def get_word_nodes(self) -> Iterable[_Element]:
        """Get the `.ocrx_word` child elements of this line."""
        return self.element.xpath('.//html:*[@class="ocrx_word"]', namespaces=XMLNS)

    def get_word(self, node: _Element) -> 'Word':
        """Get a `Word` object wrapping the given `.ocrx_word` element."""
        return Word(node, self.scale)


class Word(HOCRRegion, WordRegion):
    """Wraps an hOCR `.ocrx_word` element."""

    def get_content(self) -> str:
        """Get the string content of this word's element, and any child elements."""
        return ''.join(self.element.itertext())
