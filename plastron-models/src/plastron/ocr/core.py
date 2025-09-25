from typing import NamedTuple, Iterator, TypeVar, Optional, Iterable

# noinspection PyProtectedMember
from lxml.etree import _Element


class XYWH(NamedTuple):
    """Rectangular region defined by its top-left corner (x, y) coordinates
    plus its width and height (w, h).

    ```pycon
    >>> from plastron.ocr.core import XYWH

    >>> region = XYWH(100, 120, 50, 60)

    >>> str(region)
    '100,120,50,60'

    ```
    """
    x: int
    """X-axis coordinate of the top-left corner of the region"""
    y: int
    """Y-axis coordinate of the top-left corner of the region"""
    w: int
    """Width of the region (i.e., size along the X-axis)"""
    h: int
    """Height of the region (i.e., size along the Y-axis)"""

    @classmethod
    def from_bbox(cls, bbox: 'BBox'):
        """Creates an `XYWH` object representing the same region as the given
        `BBox` object."""
        return cls(x=bbox.x1, y=bbox.y1, w=bbox.x2 - bbox.x1, h=bbox.y2 - bbox.y1)

    def __str__(self):
        return ','.join(map(str, (self.x, self.y, self.w, self.h)))

    def scale(self, scale: 'Scale') -> 'XYWH':
        """Return a new `XYWH` with coordinates adjusted by the given `Scale`."""
        return XYWH(
            x=round(self.x * scale.x),
            y=round(self.y * scale.y),
            w=round(self.w * scale.x),
            h=round(self.h * scale.y),
        )


class BBox(NamedTuple):
    """Rectangular region defined by its top-left (x1, y1) and bottom-right
    (x2, y2) coordinates.

    ```pycon
    >>> from plastron.ocr.core import BBox

    >>> region = BBox(100, 120, 150, 180)

    >>> str(region)
    '100,120,150,180'

    ```
    """
    x1: int
    """X-axis coordinate of the top-left corner of the region"""
    y1: int
    """Y-axis coordinate of the top-left corner of the region"""
    x2: int
    """X-axis coordinate of the bottom-right corner of the region"""
    y2: int
    """Y-axis coordinate of the bottom-right corner of the region"""

    @classmethod
    def from_xywh(cls, xywh: XYWH):
        """Creates a `BBox` object representing the same region as the given
        `XYWH` object."""
        return cls(x1=xywh.x, y1=xywh.y, x2=xywh.x + xywh.w, y2=xywh.y + xywh.h)

    def __str__(self):
        return ','.join(map(str, (self.x1, self.y1, self.x2, self.y2)))

    def scale(self, scale: 'Scale') -> 'BBox':
        """Return a new `BBox` with coordinates adjusted by the given `Scale`."""
        return BBox(
            x1=round(self.x1 * scale.x),
            y1=round(self.y1 * scale.y),
            x2=round(self.x2 * scale.x),
            y2=round(self.y2 * scale.y),
        )


class Scale(NamedTuple):
    """Conversion factor between a given measurement unit and pixels, given the
    image resolution in pixels per inch (a.k.a. DPI). Supported measurement units
    are:

    * `inch1200` (1/1200 of an inch)
    * `mm10` (1/10 of a millimeter)
    * `pixel`
    """
    x: float
    """Horizontal scaling factor"""
    y: float
    """Vertical scaling factor"""

    @classmethod
    def from_resolution(cls, image_resolution: tuple[int, int], unit: str = 'pixel'):
        xres = image_resolution[0]
        yres = image_resolution[1]
        if unit == 'inch1200':
            return cls(xres / 1200.0, yres / 1200.0)
        elif unit == 'mm10':
            return cls(xres / 254.0, yres / 254.0)
        elif unit == 'pixel':
            return cls(1, 1)
        else:
            raise ValueError(f"Unknown MeasurementUnit '{unit}'")


B = TypeVar('B', bound='BlockRegion')
"""`BlockRegion` type"""

L = TypeVar('L', bound='LineRegion')
"""`LineRegion` type"""

W = TypeVar('W', bound='WordRegion')
"""`WordRegion` type"""


class OCRResource:
    """Base class for classes representing complete OCR XML files.

    Conceptually, an OCRResource has the following abstract structure:

    * Resource has 0-or-more Blocks
    * Block has 0-or-more Lines
    * Line has 0-or-more Words

    Specific concrete classes may have other data structures not covered
    here.

    Subclasses must implement three methods:

    * `get_block_nodes()`
    * `get_block_node()`
    * `get_block()`
    """
    def __iter__(self) -> Iterator[B]:
        return self.blocks

    @property
    def blocks(self) -> Iterator[B]:
        """Iterator over the blocks for this resource."""
        for node in self.get_block_nodes():
            yield self.get_block(node)

    def block(self, identifier: str) -> Optional[B]:
        """Retrieve an individual block by identifier."""
        try:
            return self.get_block(self.get_block_node(identifier))
        except IndexError:
            return None

    def get_block_nodes(self) -> Iterable[_Element]:
        """Return an iterable of XML elements representing block-level
        subdivisions of the OCR resource.

        *Must be implemented by subclasses.*"""
        raise NotImplementedError

    def get_block_node(self, identifier: str) -> _Element:
        """Return a single XML element, selected by the `identifier`.

        *Must be implemented by subclasses.*"""
        raise NotImplementedError

    def get_block(self, node: _Element) -> B:
        """Return a `BlockRegion` subclass that encapsulates the
        given XML element `node`.

        *Must be implemented by subclasses.*"""
        raise NotImplementedError

    def words(self) -> Iterator[W]:
        """Iterator over all `WordRegion` elements contained in
        this OCR resource."""
        for block in self.blocks:
            for line in block.lines():
                for word in line.words():
                    yield word


class RegionBase:
    """Base class for classes representing regions of a page with
    a specified bounding box. Subclasses should populate the `_xywh`
    and `_bbox` attributes used to back the `xywh` and `bbox`
    properties."""

    _xywh: XYWH
    _bbox: BBox

    @property
    def xywh(self):
        """Bounding box as `XYWH` (X, Y, Width, Height) coordinates."""
        return self._xywh

    @property
    def bbox(self):
        """Bounding box as `BBox` (Top-left and bottom-right X, Y) coordinates."""
        return self._bbox


class BlockRegion(RegionBase):
    """Base class for classes representing a block-like region of
    a page (e.g., ALTO `TextBlock` or hOCR `ocr_carea`)."""

    def __iter__(self):
        return self.lines

    def __str__(self):
        return ''.join(str(s) + '\n' for s in self.lines())

    def lines(self) -> Iterator[L]:
        """Iterator returning the content of this block region
        as individual lines."""
        for node in self.get_line_nodes():
            yield self.get_line(node)

    def get_line_nodes(self) -> Iterable[_Element]:
        """Return an iterable of XML elements representing the
        lines within this block.

        *Must be implemented by subclasses.*"""
        raise NotImplementedError

    def get_line(self, node: _Element) -> L:
        """Return a `LineRegion` subclass that encapsulates the
        given XML element `node`.

        *Must be implemented by subclasses.*"""
        raise NotImplementedError


class LineRegion(RegionBase):
    """Base class for classes representing a single line in an OCR
    resource (e.g., ALTO `TextLine` or hOCR `ocr_line`)."""
    def __str__(self):
        return ' '.join(str(w) for w in self.words())

    def __iter__(self):
        return self.words

    def words(self) -> Iterator[W]:
        """Iterator returning the content of this line as individual
        words."""
        for node in self.get_word_nodes():
            yield self.get_word(node)

    def get_word_nodes(self) -> Iterable[_Element]:
        """Return an iterable of XML elements representing the
        words within this line.

        *Must be implemented by subclasses.*"""
        raise NotImplementedError

    def get_word(self, node: _Element) -> W:
        """Return a `WordRegion` subclass that encapsulates the
        given XML element `node`.

        *Must be implemented by subclasses.*"""
        raise NotImplementedError


class WordRegion(RegionBase):
    """Base class for classes representing a single word within the
    OCR resource."""
    def __str__(self):
        return self.content

    @property
    def content(self) -> str:
        """The string content of this word."""
        return self.get_content()

    def get_content(self) -> str:
        """Return the string content of this word.

        *Must be implemented by subclasses.*"""
        raise NotImplementedError


class OCRError(Exception):
    """OCR-related error"""
    pass


class OCRFileError(OCRError):
    pass


class ImageFileError(OCRError):
    pass


class UnrecognizedOCRFormatError(OCRError):
    pass
