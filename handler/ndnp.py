''' Classes for interpreting and loading metadata and files stored
    according to the NDNP specification. '''

from classes import pcdm, ocr
import csv
import logging
import lxml.etree as ET
import os
import rdflib
from rdflib import Namespace, URIRef, RDF
import requests
import sys
import mimetypes

#============================================================================
# NAMESPACE BINDINGS
#============================================================================

namespace_manager = rdflib.namespace.NamespaceManager(rdflib.Graph())

bibo = Namespace('http://purl.org/ontology/bibo/')
namespace_manager.bind('bibo', bibo, override=False)

carriers = Namespace('http://id.loc.gov/vocabulary/carriers/')
namespace_manager.bind('carriers', carriers, override=False)

dc = Namespace('http://purl.org/dc/elements/1.1/')
namespace_manager.bind('dc', dc, override=False)

dcmitype = Namespace('http://purl.org/dc/dcmitype/')
namespace_manager.bind('dcmitype', dcmitype, override=False)

dcterms = Namespace('http://purl.org/dc/terms/')
namespace_manager.bind('dcterms', dcterms, override=False)

ebucore = Namespace('http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#')
namespace_manager.bind('ebucore', ebucore, override=False)

fabio = Namespace('http://purl.org/spar/fabio/')
namespace_manager.bind('fabio', fabio, override=False)

foaf = Namespace('http://xmlns.com/foaf/0.1/')
namespace_manager.bind('foaf', foaf, override=False)

iana = Namespace('http://www.iana.org/assignments/relation/')
namespace_manager.bind('iana', iana, override=False)

ndnp = Namespace('http://chroniclingamerica.loc.gov/terms/')
namespace_manager.bind('ndnp', ndnp, override=False)

ore = Namespace('http://www.openarchives.org/ore/terms/')
namespace_manager.bind('ore', ore, override=False)

pcdm_ns = Namespace('http://pcdm.org/models#')
namespace_manager.bind('pcdm', pcdm_ns, override=False)

pcdm_use = Namespace('http://pcdm.org/use#')
namespace_manager.bind('pcdmuse', pcdm_use, override=False)

rdf = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
namespace_manager.bind('rdf', rdf, override=False)

oa = Namespace('http://www.w3.org/ns/oa#')
namespace_manager.bind('oa', oa, override=False)

sc = Namespace('http://www.shared-canvas.org/ns/')
namespace_manager.bind('sc', sc, override=False)

prov = Namespace('http://www.w3.org/ns/prov#')
namespace_manager.bind('prov', prov, override=False)


#============================================================================
# METADATA MAPPING
#============================================================================

XPATHMAP = {
    'batch': {
        'issues':   "./{http://www.loc.gov/ndnp}issue",
        'reels':    "./{http://www.loc.gov/ndnp}reel"
        },

    'issue': {
        'volume':   (".//{http://www.loc.gov/mods/v3}detail[@type='volume']/"
                    "{http://www.loc.gov/mods/v3}number"
                    ),
        'issue':    (".//{http://www.loc.gov/mods/v3}detail[@type='issue']/"
                    "{http://www.loc.gov/mods/v3}number"
                    ),
        'edition':  (".//{http://www.loc.gov/mods/v3}detail[@type='edition']/"
                    "{http://www.loc.gov/mods/v3}number"
                    ),
        'article':  (".//{http://www.loc.gov/METS/}div[@TYPE='article']"
                    ),
        'areas':    (".//{http://www.loc.gov/METS/}area"
                    ),
        }
    }

xmlns = {
    'METS': 'http://www.loc.gov/METS/',
    'mix': 'http://www.loc.gov/mix/',
    'MODS': 'http://www.loc.gov/mods/v3',
    'premis': 'http://www.loc.gov/standards/premis',
    'xlink': 'http://www.w3.org/1999/xlink',
}

#============================================================================
# DATA LOADING FUNCTION
#============================================================================

def load(repo, batch_config):
    return Batch(repo, batch_config)

#============================================================================
# EXCEPTION CLASSES
#============================================================================

class ConfigException(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class DataReadException(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

#============================================================================
# NDNP BATCH CLASS
#============================================================================

class Batch():

    '''iterator class representing the set of resources to be loaded'''

    def __init__(self, repo, config):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )
        self.batchfile = config.get('LOCAL_PATH')
        collection_uri = config.get('COLLECTION')
        if collection_uri is None:
            raise ConfigException(
                'Missing required key COLLECTION in batch config'
                )
        self.collection = Collection.from_repo_uri(repo, collection_uri)

        self.fieldnames = ['aggregation', 'sequence', 'uri']

        try:
            tree = ET.parse(self.batchfile)
        except OSError as e:
            raise DataReadException("Unable to read {0}".format(self.batchfile))
        except ET.XMLSyntaxError as e:
            raise DataReadException("Unable to parse {0} as XML".format(self.batchfile))

        root = tree.getroot()
        m = XPATHMAP

        # read over the index XML file assembling a list of paths to the issues
        self.basepath = os.path.dirname(self.batchfile)
        self.issues = []
        for i in root.findall(m['batch']['issues']):
            sanitized_path = i.text[:-6] + i.text[-4:]
            self.issues.append(
                (os.path.join(self.basepath, i.text),
                 os.path.join(
                    self.basepath, "Article-Level", sanitized_path)
                    )
                )

        # set up a CSV file for each reel, skipping existing CSVs
        self.reels = set(
            [r.get('reelNumber') for r in root.findall(m['batch']['reels'])]
            )
        self.logger.info('Batch contains {0} reels'.format(len(self.reels)))
        self.path_to_reels = os.path.join(config.get('LOG_LOCATION'), 'reels')
        if not os.path.isdir(self.path_to_reels):
            os.makedirs(self.path_to_reels)
        for n, reel in enumerate(self.reels):
            reel_csv = '{0}/{1}.csv'.format(self.path_to_reels, reel)
            if not os.path.isfile(reel_csv):
                self.logger.info(
                    "{0}. Creating reel aggregation CSV in '{1}'".format(
                        n+1, reel_csv)
                    )
                with open(reel_csv, 'w') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                    writer.writeheader()
            else:
                self.logger.info(
                    "{0}. Reel aggregation file '{1}' exists; skipping".format(
                        n+1, reel_csv)
                    )

        self.length = len(self.issues)
        self.num = 0
        self.logger.info("Batch contains {0} items.".format(self.length))

    def __iter__(self):
        return self

    def __next__(self):
        if self.num < self.length:
            data = self.issues[self.num]
            issue = Issue(self, data)
            issue.add_collection(self.collection)
            self.num += 1
            return issue
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()

#============================================================================
# NDNP ISSUE OBJECT
#============================================================================

class Issue(pcdm.Item):

    ''' class representing all components of a newspaper issue '''

    def __init__(self, batch, paths):
        print('\n' + '*' * 80)
        (issue_path, article_path) = paths
        print(issue_path)
        super(Issue, self).__init__()

        # gather metadata
        self.dir            = os.path.dirname(issue_path)
        self.path           = issue_path
        self.article_path   = article_path
        self.reel_csv_loc   = batch.path_to_reels

    def read_data(self):
        try:
            tree = ET.parse(self.path)
        except OSError as e:
            raise DataReadException("Unable to read {0}".format(self.path))
        except ET.XMLSyntaxError as e:
            raise DataReadException(
                "Unable to parse {0} as XML".format(self.path)
                )

        issue_mets = METSResource(tree)
        root = tree.getroot()
        m = XPATHMAP['issue']

        # get required metadata elements
        try:
            self.title = root.get('LABEL')
            self.date = root.find('.//MODS:dateIssued', xmlns).text
            self.sequence_attr = ('Page', 'number')
        except AttributeError as e:
            raise DataReadException("Missing metadata in {0}".format(self.path))

        # optional metadata elements
        if root.find(m['volume']) is not None:
            self.volume = root.find(m['volume']).text
        if root.find(m['issue']) is not None:
            self.issue = root.find(m['issue']).text
        if root.find(m['edition']) is not None:
            self.edition = root.find(m['edition']).text

        # add the issue and article-level XML files as related objects
        self.add_related(IssueMetadata(MetadataFile.from_localpath(
            localpath=self.path,
            title='{0}, issue METS metadata'.format(self.title)
            )))
        self.add_related(IssueMetadata(MetadataFile.from_localpath(
            localpath=self.article_path,
            title='{0}, article METS metadata'.format(self.title)
            )))

        # create a page object for each page and append to list of pages
        pages = {}
        for div in issue_mets.xpath('METS:structMap//METS:div[@TYPE="np:page"]'):
            page = Page.from_mets(issue_mets, div, self)
            pages[str(page.number)] = page
            self.add_component(page)

            # add OCR text blocks as annotations
            self.annotations.extend(page.textblocks())

        # iterate over the article XML and create objects for articles
        try:
            article_tree = ET.parse(self.article_path)
        except OSError as e:
            raise DataReadException(
                "Unable to read {0}".format(self.article_path)
                )
        except ET.XMLSyntaxError as e:
            raise DataReadException(
                "Unable to parse {0} as XML".format(self.article_path)
                )

        article_root = article_tree.getroot()
        for article in article_root.findall(m['article']):
            article_title = article.get('LABEL')
            article_pagenums = set()
            for area in article.findall(m['areas']):
                pagenum = int(area.get('FILEID').replace('ocrFile', ''))
                page = pages[str(pagenum)]
                article_pagenums.add(pagenum)
            article = Article(article_title, self, pages=sorted(list(article_pagenums)))
            self.add_component(article)

    def graph(self):
        graph = super(Issue, self).graph()
        # store required metadata as an RDF graph
        graph.namespace_manager = namespace_manager
        graph.add((self.uri, dcterms.title, rdflib.Literal(self.title)))
        graph.add((self.uri, dc.date, rdflib.Literal(self.date)))
        graph.add((self.uri, rdf.type, bibo.Issue))
        # add optional metadata elements if present
        if hasattr(self, 'volume'):
            graph.add((self.uri, bibo.volume, rdflib.Literal(self.volume)))
        if hasattr(self, 'issue'):
            graph.add((self.uri, bibo.issue, rdflib.Literal(self.issue)))
        if hasattr(self, 'edition'):
            graph.add((self.uri, bibo.edition, rdflib.Literal(self.edition)))
        return graph

    # actions to take upon successful creation of object in repository
    def post_creation_hook(self):
        super(Issue, self).post_creation_hook()
        for page in self.ordered_components():
            if hasattr(page, 'frame'):
                row = {'aggregation': page.reel,
                       'sequence': page.frame,
                       'uri': page.uri
                        }
                csv_path = os.path.join(
                    self.reel_csv_loc, '{0}.csv'.format(page.reel)
                    )
                with open(csv_path, 'r') as f:
                    fieldnames = f.readline().strip('\n').split(',')
                with open(csv_path, 'a') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row)
        self.logger.info('Completed post-creation actions')

class METSResource(object):
    def __init__(self, xmldoc):
        self.root = xmldoc.getroot()
        self.xpath = ET.XPathElementEvaluator(self.root, namespaces=xmlns,
                smart_strings = False)

    def dmdsec(self, id):
        return self.xpath('METS:dmdSec[@ID=$id]', id=id)[0]

    def file(self, id):
        return self.xpath('METS:fileSec//METS:file[@ID=$id]', id=id)[0]

    def techmd(self, id):
        return self.xpath('METS:amdSec/METS:techMD[@ID=$id]', id=id)[0]

class TextblockOnPage(pcdm.Annotation):
    def __init__(self, textblock, page, article=None):
        super(TextblockOnPage, self).__init__()
        body = pcdm.TextualBody(textblock.text(scale=page.ocr.scale), 'text/plain')
        if article is not None:
            body.linked_objects.append((dcterms.isPartOf, article))
        target = pcdm.SpecificResource(page)
        xywh = ','.join([ str(i) for i in textblock.xywh(page.ocr.scale) ])
        selector = pcdm.FragmentSelector(
            "xywh={0}".format(xywh),
            rdflib.URIRef('http://www.w3.org/TR/media-frags/')
            )
        xpath_selector = pcdm.XPathSelector('//*[@ID="{0}"]'.format(textblock.id))
        ocr_resource = pcdm.SpecificResource(page.ocr_file)
        ocr_resource.add_selector(xpath_selector)
        self.linked_objects.append((prov.wasDerivedFrom, ocr_resource))
        self.add_body(body)
        self.add_target(target)
        self.motivation = sc.painting
        target.add_selector(selector)
        self.fragments = [body, target, selector, ocr_resource, xpath_selector]

class IssueMetadata(pcdm.Component):
    '''additional metadata about an issue'''

    def __init__(self, file, title=None):
        super(IssueMetadata, self).__init__()
        self.add_file(file)
        if title is not None:
            self.title = title
        else:
            self.title = file.title

    def graph(self):
        graph = super(IssueMetadata, self).graph()
        graph.namespace_manager = namespace_manager
        graph.add((self.uri, rdf.type, fabio.Metadata))
        graph.add((self.uri, dcterms.title, rdflib.Literal(self.title)))
        return graph

class MetadataFile(pcdm.File):
    '''a binary file containing metadata in non-RDF formats (METS, MODS, etc.)'''

    def graph(self):
        graph = super(MetadataFile, self).graph()
        graph.namespace_manager = namespace_manager
        graph.add((self.uri, rdf.type, fabio.MetadataDocument))
        return graph

#============================================================================
# NDNP PAGE OBJECT
#============================================================================

class Page(pcdm.Component):

    ''' class representing a newspaper page '''

    @classmethod
    def from_mets(cls, issue_mets, div, issue):
        dmdsec = issue_mets.dmdsec(div.get('DMDID'))
        number = dmdsec.find('.//MODS:start', xmlns).text
        reel = dmdsec.find('.//MODS:identifier[@type="reel number"]', xmlns).text
        frame = dmdsec.find('.//MODS:identifier[@type="reel sequence number"]', xmlns)
        if frame is not None:
            frame = frame.text
        title = "{0}, page {1}".format(issue.title, number)

        # create Page object
        page = cls(issue, reel, number, title, frame)

        # optionally generate a file object for each file in the XML snippet
        for fptr in div.findall('METS:fptr', xmlns):
            fileid = fptr.get('FILEID')
            filexml = issue_mets.file(fileid)

            # get technical metadata by type
            techmd = {}
            for admid in filexml.get('ADMID').split():
                t = issue_mets.techmd(admid)
                for mdwrap in t.findall('METS:mdWrap', xmlns):
                    mdtype = mdwrap.get('MDTYPE')
                    if mdtype == 'OTHER':
                        mdtype = mdwrap.get('OTHERMDTYPE')
                techmd[mdtype] = t

            file = File.from_mets(filexml, issue.dir, techmd)
            page.add_file(file)

        page.parse_ocr()

        return page

    @classmethod
    def from_repository(cls, repo, page_uri, graph=None):
        # insert transaction URI into the page_uri, since the returned
        # graph will have the transaction URI in all of its URIs
        page_uri = rdflib.URIRef(repo._insert_transaction_uri(page_uri))

        if graph is None:
            page_graph = repo.get_graph(page_uri)
        else:
            page_graph = graph

        title = page_graph.value(subject=page_uri, predicate=dcterms.title)
        number = page_graph.value(subject=page_uri, predicate=ndnp.number)
        frame = page_graph.value(subject=page_uri, predicate=ndnp.frame)

        #TODO: real value for issue and reel
        page = cls(issue=None, reel=None, number=number, title=title, frame=frame)
        page.uri = page_uri
        page.created = True
        page.updated = True

        for file_uri in page_graph.objects(subject=page_uri, predicate=pcdm_ns.hasFile):
            file = File.from_repository(repo, file_uri)
            page.add_file(file)

        page.parse_ocr()

        return page


    def __init__(self, issue, reel, number, title=None, frame=None):
        super(Page, self).__init__()
        self.issue = issue
        self.reel = reel
        self.number = number
        self.title = title
        self.frame = frame
        self.ordered = True

    def parse_ocr(self):
        ocr_file = next(self.files_for('ocr'))
        # load ALTO XML into page object, for text extraction
        try:
            with ocr_file.open_stream() as stream:
                tree = ET.parse(stream)
        except OSError as e:
            raise DataReadException("Unable to read {0}".format(ocr_file.filename))
        except ET.XMLSyntaxError as e:
            raise DataReadException("Unable to parse {0} as XML".format(ocr_file.filename))

        # read in resolution from issue METS data
        master = next(self.files_for('master'))
        self.ocr_file = ocr_file
        self.ocr = ocr.ALTOResource(tree, master.resolution)

    def textblocks(self):
        # extract text blocks from ALTO XML for this page
        for textblock in self.ocr.textblocks():
            yield TextblockOnPage(textblock, self)

    def graph(self):
        graph = super(Page, self).graph()
        graph.namespace_manager = namespace_manager
        graph.add((self.uri, dcterms.title, rdflib.Literal(self.title)))
        graph.add((self.uri, pcdm_ns.memberOf, self.issue.uri))
        graph.add((self.uri, rdf.type, ndnp.Page))
        # add optional metadata elements if present
        if hasattr(self, 'number'):
            graph.add((self.uri, ndnp.number, rdflib.Literal(self.number)))
        if hasattr(self, 'frame'):
            graph.add((self.uri, ndnp.sequence, rdflib.Literal(self.frame)))
        return graph

    def files_for(self, use):
        for f in self.files():
            if f.use == use:
                yield f

#============================================================================
# NDNP FILE OBJECT
#============================================================================

class File(pcdm.File):

    ''' class representing an individual file '''

    @classmethod
    def from_mets(cls, filexml, base_dir, techmd):
        use = filexml.get('USE')
        file_locator = filexml.find('METS:FLocat', xmlns)
        href = file_locator.get('{http://www.w3.org/1999/xlink}href')
        localpath = os.path.join(base_dir, os.path.basename(href))
        basename = os.path.basename(localpath)
        mimetype = techmd['PREMIS'].find('.//premis:formatName', xmlns).text
        file = cls.from_localpath(
            localpath,
            mimetype=mimetype,
            title="{0} ({1})".format(basename, use)
            )
        file.use = use
        file.basename = basename

        if mimetype == 'image/tiff':
            file.width = techmd['NISOIMG'].find('.//mix:ImageWidth', xmlns).text
            file.height = techmd['NISOIMG'].find('.//mix:ImageLength', xmlns).text
            file.resolution = (
                int(techmd['NISOIMG'].find('.//mix:XSamplingFrequency', xmlns).text),
                int(techmd['NISOIMG'].find('.//mix:YSamplingFrequency', xmlns).text)
                )
        else:
            file.width = None
            file.height = None
            file.resolution = None

        return file

    @classmethod
    def from_repository(cls, repo, file_uri):
        head_res = repo.head(file_uri)
        if 'describedby' in head_res.links:
            rdf_uri = head_res.links['describedby']['url']
            file_res = repo.get(rdf_uri, headers={'Accept': 'text/turtle'})
            file_graph = rdflib.Graph()
            file_graph.parse(data=file_res.text, format='turtle')

            title = file_graph.value(subject=file_uri, predicate=dcterms.title)
            mimetype = file_graph.value(subject=file_uri,
                    predicate=ebucore.hasMimeType)
            filename = file_graph.value(subject=file_uri,
                    predicate=ebucore.filename)

            def open_stream():
                return repo.get(file_uri, stream=True).raw

            file = cls(
                filename=filename,
                mimetype=mimetype,
                title=title,
                open_stream=open_stream
                )
            file.uri = file_uri
            file.created = True
            file.updated = True

            types = list(file_graph.objects(subject=file_uri, predicate=RDF.type))
            if pcdm_use.PreservationMasterFile in types:
                file.use = 'master'
            elif pcdm_use.IntermediateFile in types:
                file.use = 'service'
            elif pcdm_use.ServiceFile in types:
                file.use = 'derivative'
            elif pcdm_use.ExtractedText in types:
                file.use = 'ocr'

            if file.use == 'master':
                file.width = file_graph.value(subject=file_uri, predicate=ebucore.width)
                file.height = file_graph.value(subject=file_uri, predicate=ebucore.height)
                #TODO: how to not hardocde this?
                file.resolution = (400,400)

            return file
        else:
            raise Exception("No metadata for resource")

    def graph(self):
        graph = super(File, self).graph()
        graph.namespace_manager = namespace_manager
        graph.add((self.uri, dcterms.title, rdflib.Literal(self.title)))
        graph.add((self.uri, dcterms.type, dcmitype.Text))

        if self.width is not None:
            graph.add((self.uri, ebucore.width, rdflib.Literal(self.width)))
        if self.height is not None:
            graph.add((self.uri, ebucore.height, rdflib.Literal(self.height)))

        if self.basename.endswith('.tif'):
            graph.add((self.uri, rdf.type, pcdm_use.PreservationMasterFile))
        elif self.basename.endswith('.jp2'):
            graph.add((self.uri, rdf.type, pcdm_use.IntermediateFile))
        elif self.basename.endswith('.pdf'):
            graph.add((self.uri, rdf.type, pcdm_use.ServiceFile))
        elif self.basename.endswith('.xml'):
            graph.add((self.uri, rdf.type, pcdm_use.ExtractedText))

        return graph

#============================================================================
# NDNP COLLECTION OBJECT
#============================================================================

class Collection(pcdm.Collection):

    ''' class representing a collection of newspaper resources '''

    def __init__(self):
        super(Collection, self).__init__()

    @classmethod
    def from_repo_uri(klass, repository, uri):
        response = repository.get(uri, headers={'Accept': 'application/rdf+xml'})
        if response.status_code == 200:
            graph = rdflib.graph.Graph().parse(data=response.text)
            collection = klass()
            collection.uri = rdflib.URIRef(uri)
            # mark as created and updated so that the create_object and update_object
            # methods doesn't try try to modify it
            collection.created = True
            collection.updated = True

            # default title is the URI
            collection.title = str(collection.uri)
            for o in graph.objects(subject=collection.uri, predicate=dcterms.title):
                collection.title = str(o)
        else:
            raise ConfigException(
                "Collection URI {0} could not be reached.".format(collection.uri)
                )

        return collection


#============================================================================
# NDNP ARTICLE OBJECT
#============================================================================

class Article(pcdm.Component):

    ''' class representing an article in a newspaper issue '''

    def __init__(self, title, issue, pages=None):
        super(Article, self).__init__()

        # gather metadata
        self.title = title
        self.issue = issue
        self.ordered = False
        if pages is not None:
            self.start_page = pages[0]
            self.end_page = pages[-1]

    def graph(self):
        graph = super(Article, self).graph()
        graph.namespace_manager = namespace_manager
        graph.add((self.uri, dcterms.title, rdflib.Literal(self.title)))
        graph.add((self.uri, pcdm_ns.memberOf, self.issue.uri))
        graph.add((self.uri, rdf.type, bibo.Article))
        if self.start_page is not None:
            graph.add((self.uri, bibo.pageStart, rdflib.Literal(self.start_page)))
        if self.end_page is not None:
            graph.add((self.uri, bibo.pageEnd, rdflib.Literal(self.end_page)))
        return graph
