from lxml.etree import parse, XMLSyntaxError

from plastron.handles import HandleBearingResource
from plastron.models import ContentModeledResource
from plastron.models.annotations import TextblockOnPage
from plastron.models.authorities import UMD_TERMS_OF_USE_STATEMENTS, UMD_PRESENTATION_SETS, UMD_RIGHTS_STATEMENTS, Agent
from plastron.models.fedora import FedoraResource
from plastron.models.page import Page, File
from plastron.models.pcdm import PCDMObject, PCDMFile
from plastron.namespaces import bibo, carriers, dc, dcterms, fabio, ndnp, ore, pcdm, pcdmuse, schema, umd
from plastron.ocr.alto import ALTOResource
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.validation.rules import is_iso_8601_date
from plastron.validation.vocabularies import ControlledVocabularyProperty


@rdf_type(bibo.Issue, umd.Newspaper)
class Issue(ContentModeledResource, PCDMObject, HandleBearingResource, FedoraResource):
    """Newspaper issue"""
    model_name = 'Issue'
    is_top_level = True

    member_of = ObjectProperty(pcdm.memberOf)
    has_member = ObjectProperty(pcdm.hasMember, repeatable=True, cls=Page)
    has_file = ObjectProperty(pcdm.hasFile, repeatable=True, cls=File)
    title = DataProperty(dcterms.title, required=True)
    date = DataProperty(dc.date, required=True, validate=is_iso_8601_date)
    volume = DataProperty(bibo.volume, required=True)
    issue = DataProperty(bibo.issue, required=True)
    edition = DataProperty(bibo.edition, required=True)
    presentation_set = ControlledVocabularyProperty(ore.isAggregatedBy, repeatable=True, vocab=UMD_PRESENTATION_SETS)
    copyright_notice = DataProperty(schema.copyrightNotice)
    terms_of_use = ControlledVocabularyProperty(dcterms.license, vocab=UMD_TERMS_OF_USE_STATEMENTS)
    rights = ControlledVocabularyProperty(dcterms.rights, required=True, vocab=UMD_RIGHTS_STATEMENTS)
    rights_holder = ObjectProperty(dcterms.rightsHolder, repeatable=True, embed=True, cls=Agent)

    HEADER_MAP = {
        'title': 'Title',
        'date': 'Date',
        'volume': 'Volume',
        'issue': 'Issue',
        'edition': 'Edition',
        'handle': 'Handle',
        'presentation_set': 'Presentation Set',
        'copyright_notice': 'Copyright Notice',
        'terms_of_use': 'Terms of Use',
        'rights': 'Rights Statement',
        'rights_holder': {
            'label': 'Rights Holder',
        },
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
            raise RuntimeError("Unable to read {0}".format(ocr_file.filename))
        except XMLSyntaxError:
            raise RuntimeError("Unable to parse {0} as XML".format(ocr_file.filename))

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
