from PIL import Image
from lxml import etree

from plastron.ocr.alto import ALTOResource
from plastron.ocr.core import OCRError, UnrecognizedOCRFormatError, OCRResource
from plastron.ocr.hocr import HOCRResource
from plastron.repo import BinaryResource


class ImageWithOCR:
    def __init__(self, image_file: BinaryResource, ocr_file: BinaryResource):
        self.image_file = image_file
        self.ocr_file = ocr_file

    def get_ocr_resource(self) -> OCRResource:
        with self.ocr_file.open() as fh:
            doc = etree.parse(fh)

        with self.image_file.open() as fh:
            img = Image.open(fh)
            resolution = img.info['dpi']

        root = doc.getroot()
        if root.tag == '{http://www.loc.gov/standards/alto/ns-v2#}alto':
            # ALTO XML
            return ALTOResource(doc, resolution)
        elif root.tag == '{http://www.w3.org/1999/xhtml}html':
            # hOCR
            return HOCRResource(doc, resolution)
        else:
            raise UnrecognizedOCRFormatError
