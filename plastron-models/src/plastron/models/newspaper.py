import re

from lxml.etree import parse, XMLSyntaxError

from plastron.models.annotations import TextblockOnPage
from plastron.models.umd import PCDMObject, PCDMFile
from plastron.namespaces import bibo, carriers, dc, dcterms, fabio, ndnp, pcdmuse, umdtype, pcdm, umd
from plastron.rdf.ocr import ALTOResource
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.repo import DataReadError
from plastron.validation import is_handle


def is_iso_8601_date(value: str) -> bool:
    """an ISO 8601 date string (YYYY-MM-DD)"""
    return bool(re.match(r'^\d\d\d\d-\d\d-\d\d$', value))


@rdf_type(bibo.Issue, umd.Newspaper)
class Issue(PCDMObject):
    """Newspaper issue"""
    title = DataProperty(dcterms.title, required=True)
    date = DataProperty(dc.date, required=True, validate=is_iso_8601_date)
    volume = DataProperty(bibo.volume, required=True)
    issue = DataProperty(bibo.issue, required=True)
    edition = DataProperty(bibo.edition, required=True)
    handle = DataProperty(dcterms.identifier, datatype=umdtype.handle, validate=is_handle)

    HEADER_MAP = {
        'title': 'Title',
        'date': 'Date',
        'volume': 'Volume',
        'issue': 'Issue',
        'edition': 'Edition',
        'handle': 'Handle'
    }


@rdf_type(fabio.Metadata)
class IssueMetadata(PCDMObject):
    """Additional metadata about an issue"""
    pass


@rdf_type(fabio.MetadataDocument)
class MetadataFile(PCDMFile):
    """A binary file containing metadata in non-RDF formats (METS, MODS, etc.)"""
    pass


@rdf_type(ndnp.Page)
class Page(PCDMObject):
    """Newspaper page"""
    issue = ObjectProperty(pcdm.memberOf, cls=Issue)
    number = DataProperty(ndnp.number)
    frame = DataProperty(ndnp.sequence)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ordered = True
        self.ocr = None
        self.ocr_file = None

    def parse_ocr(self):
        # try to get an OCR file
        # if there isn't one, just skip it
        try:
            ocr_file = next(self.files_for('ocr'))
        except StopIteration:
            return

        # load ALTO XML into page object, for text extraction
        try:
            with ocr_file.source as stream:
                tree = parse(stream)
        except OSError:
            raise DataReadError("Unable to read {0}".format(ocr_file.filename))
        except XMLSyntaxError:
            raise DataReadError("Unable to parse {0} as XML".format(ocr_file.filename))

        # read in resolution from issue METS data
        master = next(self.files_for('master'))
        self.ocr_file = ocr_file
        self.ocr = ALTOResource(tree, master.resolution)

    def textblocks(self):
        if self.ocr is None:
            raise StopIteration()
        # extract text blocks from ALTO XML for this page
        for textblock in self.ocr.textblocks():
            yield TextblockOnPage.from_textblock(textblock, page=self, scale=self.ocr.scale, ocr_file=self.ocr_file)

    def files_for(self, use):
        for f in self.files:
            if f.use == use:
                yield f


class File(PCDMFile):
    """Newspaper file"""

    @classmethod
    def from_repository(cls, client, uri, include_server_managed=True):
        obj = super().from_repository(client, uri, include_server_managed)

        types = obj.rdf_type.values
        if pcdmuse.PreservationMasterFile in types:
            obj.use = 'master'
            # TODO: how to not hardcode this?
            obj.resolution = (400, 400)
        elif pcdmuse.IntermediateFile in types:
            obj.use = 'service'
        elif pcdmuse.ServiceFile in types:
            obj.use = 'derivative'
        elif pcdmuse.ExtractedText in types:
            obj.use = 'ocr'

        return obj


@rdf_type(bibo.Article)
class Article(PCDMObject):
    """Newspaper article"""
    issue = ObjectProperty(pcdm.memberOf, cls=Issue)
    start_page = DataProperty(bibo.pageStart)
    end_page = DataProperty(bibo.pageEnd)


@rdf_type(carriers.hd)
class Reel(PCDMObject):
    """NDNP reel is an ordered sequence of frames"""
    id = DataProperty(dcterms.identifier)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sequence_attr = ('Frame', 'frame')
