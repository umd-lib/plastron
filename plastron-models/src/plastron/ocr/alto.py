"""[ALTO](https://www.loc.gov/standards/alto/) (Analyzed Layout and Text Object) OCR classes"""

from typing import Union, Iterator

# noinspection PyProtectedMember
from lxml.etree import _Element, _ElementTree, QName

from plastron.ocr.core import XYWH, BBox, Scale, RegionBase, OCRError, OCRResource, BlockRegion, LineRegion, WordRegion

XMLNS = {"alto": "http://www.loc.gov/standards/alto/ns-v2#"}


class ALTOResource(OCRResource):
    """ALTO XML OCR resource. The `scale` is determined by using the measurement
    unit given in the ALTO document combined with the image resolution passed to
    the constructor. If no measurement unit can be found, raises an `OCRError`."""

    def __init__(self, doc: _ElementTree, image_resolution: tuple[int, int]):
        self.doc = doc
        try:
            unit = self.doc.xpath('/alto:alto/alto:Description/alto:MeasurementUnit', namespaces=XMLNS)[0].text
        except IndexError:
            raise OCRError('Unable to determine measurement unit from ALTO document')
        self.scale = Scale.from_resolution(image_resolution, unit)

    def get_block_nodes(self):
        """Get the `<TextBlock>` descendant elements of this resource."""
        return self.doc.xpath("//alto:TextBlock", namespaces=XMLNS)

    def get_block_node(self, identifier: str):
        """Get the `<TextBlock>` descendant element whose `@ID` matches
        the given `identifier`."""
        return self.doc.xpath("//alto:TextBlock[@ID=$id]", id=identifier, namespaces=XMLNS)[0]

    def get_block(self, node: _Element) -> 'TextBlock':
        """Get the `TextBlock` object wrapping the given `<TextBlock>` element."""
        return TextBlock(node, self.scale)


class ALTORegion(RegionBase):
    """Region within an `ALTOResource`"""
    def __init__(self, element: _Element, scale: Scale):
        self.element = element
        self.scale = scale
        self.id = self.element.get('ID')
        self._xywh = XYWH(
            x=int(self.element.get('HPOS')),
            y=int(self.element.get('VPOS')),
            w=int(self.element.get('WIDTH')),
            h=int(self.element.get('HEIGHT', 0)),
        ).scale(self.scale)
        self._bbox = BBox.from_xywh(self._xywh)


class TextBlock(ALTORegion, BlockRegion):
    """Wraps an ALTO `<TextBlock>` element."""

    def get_line_nodes(self):
        """Get the `<TextLine>` child elements of this block."""
        return self.element.xpath('alto:TextLine', namespaces=XMLNS)

    def get_line(self, node: _Element):
        """Get a `TextLine` object wrapping the given `<TextLine>` element."""
        return TextLine(node, self.scale)


class TextLine(ALTORegion, LineRegion):
    """Wraps an ALTO `<TextLine>` element."""

    def get_word_nodes(self):
        """Get the `<String>` child elements of this line."""
        return self.element.xpath('alto:String', namespaces=XMLNS)

    def get_word(self, node: _Element) -> 'String':
        """Get a `String` object wrapping the given `<String>` element."""
        return String(node, self.scale)

    @property
    def inlines(self) -> Iterator[Union['String', 'Space', 'Hyphen']]:
        """Iterator over all the inline elements for this line (strings,
        spaces, and hyphens)."""

        for node in self.element.xpath('alto:String|alto:SP|alto:HYP', namespaces=XMLNS):
            tag = QName(node.tag)
            if tag.localname == 'String':
                yield String(node, self.scale)
            elif tag.localname == 'SP':
                yield Space(node, self.scale)
            elif tag.localname == 'HYP':
                yield Hyphen(node, self.scale)


class String(ALTORegion, WordRegion):
    """Wraps an ALTO `<String>` element."""

    def __str__(self):
        return self.content

    def get_content(self) -> str:
        """Get the string content.

        For indexing purposes, if there is a hyphenated word across multiple
        bounding boxes, we create two tokens, each with the full substituted
        content (i.e., the unbroken word) and their respective bounding boxes."""
        if 'SUBS_CONTENT' in self.element.attrib:
            return self.element.get('SUBS_CONTENT')
        else:
            return self.element.get('CONTENT', '')


class Space(ALTORegion):
    def __str__(self):
        return ' '


class Hyphen(ALTORegion):
    def __str__(self):
        return '\N{SOFT HYPHEN}'
