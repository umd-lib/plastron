from lxml.etree import parse, XMLSyntaxError
from rdflib import URIRef
from plastron import pcdm, ocr, oa, rdf
from plastron.exceptions import DataReadException
from plastron.namespaces import bibo, carriers, dc, dcterms, ebucore, fabio, ndnp, pcdmuse, prov, sc


@rdf.data_property('title', dcterms.title)
@rdf.data_property('date', dc.date)
@rdf.data_property('volume', bibo.volume)
@rdf.data_property('issue', bibo.issue)
@rdf.data_property('edition', bibo.edition)
@rdf.rdf_class(bibo.Issue)
class Issue(pcdm.Object):
    """Newspaper issue"""
    HEADER_MAP = {
        'title': 'Title',
        'date': 'Date',
        'volume': 'Volume',
        'issue': 'Issue',
        'edition': 'Edition'
    }
    VALIDATION_RULESET = {
        'title': {
            'required': True,
            'exactly': 1
        },
        'date': {
            'required': True,
            'exactly': 1,
            'value_pattern': r'^\d\d\d\d-\d\d-\d\d$'
        },
        'volume': {
            'required': True,
            'exactly': 1
        },
        'issue': {
            'required': True,
            'exactly': 1
        },
        'edition': {
            'required': True,
            'exactly': 1
        }
    }


@rdf.object_property('derived_from', prov.wasDerivedFrom, embed=True)
class TextblockOnPage(oa.Annotation):
    def __init__(self, textblock, page):
        super().__init__()
        self.add_body(
            oa.TextualBody(
                value=textblock.text(scale=page.ocr.scale),
                content_type='text/plain'
            )
        )
        xywh = ','.join([str(i) for i in textblock.xywh(page.ocr.scale)])
        self.add_target(
            oa.SpecificResource(
                source=page,
                selector=[oa.FragmentSelector(
                    value=f'xywh={xywh}',
                    conforms_to=URIRef('http://www.w3.org/TR/media-frags/')
                )]
            )
        )
        self.derived_from = oa.SpecificResource(
            source=page.ocr_file,
            selector=[oa.XPathSelector(value=f'//*[@ID="{textblock.id}"]')]
        )
        self.motivation = sc.painting


@rdf.rdf_class(fabio.Metadata)
class IssueMetadata(pcdm.Object):
    """Additional metadata about an issue"""

    def __init__(self, file, title=None):
        super(IssueMetadata, self).__init__()
        self.add_file(file)
        if title is not None:
            self.title = title
        else:
            self.title = file.title


@rdf.rdf_class(fabio.MetadataDocument)
class MetadataFile(pcdm.File):
    """A binary file containing metadata in non-RDF formats (METS, MODS, etc.)"""
    pass


@rdf.object_property('issue', pcdm.ns.memberOf)
@rdf.data_property('number', ndnp.number)
@rdf.data_property('frame', ndnp.sequence)
@rdf.rdf_class(ndnp.Page)
class Page(pcdm.Object):
    """Newspaper page"""

    @classmethod
    def from_repository(cls, repo, page_uri):
        page = cls.from_graph(repo.get_graph(page_uri))
        page.uri = page_uri
        page.created = True
        page.updated = True

        # map file URIs to File objects
        page.files = list(map(lambda f: File.from_repository(repo, f), page.files))

        page.parse_ocr()

        return page

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
            raise DataReadException("Unable to read {0}".format(ocr_file.filename))
        except XMLSyntaxError:
            raise DataReadException("Unable to parse {0} as XML".format(ocr_file.filename))

        # read in resolution from issue METS data
        master = next(self.files_for('master'))
        self.ocr_file = ocr_file
        self.ocr = ocr.ALTOResource(tree, master.resolution)

    def textblocks(self):
        if self.ocr is None:
            raise StopIteration()
        # extract text blocks from ALTO XML for this page
        for textblock in self.ocr.textblocks():
            yield TextblockOnPage(textblock, self)

    def files_for(self, use):
        for f in self.files:
            if f.use == use:
                yield f


class File(pcdm.File):
    """Newspaper file"""

    @classmethod
    def from_repository(cls, repo, uri, include_server_managed=True):
        obj = super().from_repository(repo, uri, include_server_managed)

        types = obj.rdf_type.values
        if pcdmuse.PreservationMasterFile in types:
            obj.use = 'master'
            # TODO: how to not hardocde this?
            obj.resolution = (400, 400)
        elif pcdmuse.IntermediateFile in types:
            obj.use = 'service'
        elif pcdmuse.ServiceFile in types:
            obj.use = 'derivative'
        elif pcdmuse.ExtractedText in types:
            obj.use = 'ocr'

        return obj


@rdf.object_property('issue', pcdm.ns.memberOf)
@rdf.data_property('start_page', bibo.pageStart)
@rdf.data_property('end_page', bibo.pageEnd)
@rdf.rdf_class(bibo.Article)
class Article(pcdm.Object):
    """Newspaper article"""

    def __init__(self, pages=None, **kwargs):
        super().__init__(**kwargs)
        if pages is not None:
            self.start_page = pages[0]
            self.end_page = pages[-1]


@rdf.data_property('id', dcterms.identifier)
@rdf.rdf_class(carriers.hd)
class Reel(pcdm.Object):
    """NDNP reel is an ordered sequence of frames"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sequence_attr = ('Frame', 'frame')
