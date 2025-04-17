from PIL import Image, UnidentifiedImageError
from lxml import etree
from lxml.etree import XMLSyntaxError

from plastron.ocr.alto import ALTOResource
from plastron.ocr.core import OCRError, UnrecognizedOCRFormatError, OCRResource, ImageFileError, OCRFileError
from plastron.ocr.hocr import HOCRResource
from plastron.repo import BinaryResource, RepositoryError


class ImageWithOCR:
    def __init__(self, image_file: BinaryResource, ocr_file: BinaryResource):
        self.image_file = image_file
        self.ocr_file = ocr_file

    def get_ocr_resource(self) -> OCRResource:
        if self.ocr_file is None:
            raise OCRFileError('No OCR file specified')

        try:
            with self.ocr_file.open() as fh:
                doc = etree.parse(fh)
        except (RepositoryError, OSError, XMLSyntaxError) as e:
            raise OCRFileError(f'Cannot read OCR file {self.ocr_file.url}') from e

        if self.image_file is None:
            raise ImageFileError('No image file specified')

        try:
            with self.image_file.open() as fh:
                img = Image.open(fh)
                resolution = img.info['dpi']
        except (RepositoryError, FileNotFoundError, UnidentifiedImageError) as e:
            raise ImageFileError(f'Cannot read image file {self.image_file.url}') from e
        except KeyError:
            raise ImageFileError(f'Cannot read image resolution from {self.image_file.url}')

        root = doc.getroot()
        if root.tag == '{http://www.loc.gov/standards/alto/ns-v2#}alto':
            # ALTO XML
            return ALTOResource(doc, resolution)
        elif root.tag == '{http://www.w3.org/1999/xhtml}html':
            # hOCR
            return HOCRResource(doc, resolution)
        else:
            raise UnrecognizedOCRFormatError
