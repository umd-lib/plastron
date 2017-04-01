''' Classes for interpreting and loading metadata and files stored
    according to the NDNP specification. '''

from classes import pcdm, ocr
import csv
import logging
import lxml.etree as ET
import os
import rdflib
from rdflib import Namespace, URIRef
import requests
import sys

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

    'reel': {
        'number':   "./{http://www.loc.gov/ndnp}reel"
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
        'date':     (".//{http://www.loc.gov/mods/v3}dateIssued"
                    ),
        'lccn':     (".//{http://www.loc.gov/mods/v3}"
                    "identifier[@type='lccn']"
                    ),
        'pages':    (".//{http://www.loc.gov/METS/}dmdSec"
                    ),
        'files':    (".//{http://www.loc.gov/METS/}fileGrp"
                    ),
        'article':  (".//{http://www.loc.gov/METS/}div[@TYPE='article']"
                    ),
        'areas':    (".//{http://www.loc.gov/METS/}area"
                    ),
        'premis':   (".//{http://www.loc.gov/METS/}amdSec"
                    )
        },

    'page': {
        'number':   (".//{http://www.loc.gov/mods/v3}start"
                    ),
        'reel':     (".//{http://www.loc.gov/mods/v3}"
                    "identifier[@type='reel number']"
                    ),
        'location': (".//{http://www.loc.gov/mods/v3}physicalLocation"
                    ),
        'frame':    (".//{http://www.loc.gov/mods/v3}"
                    "identifier[@type='reel sequence number']"
                    ),
        'files':    (".//{http://www.loc.gov/METS/}file"
                    ),
        },

    'file': {
        'number':   (".//{http://www.loc.gov/mods/v3}start"
                    ),
        'filepath': (".//{http://www.loc.gov/METS/}FLocat"
                    ),
        'width':    (".//{http://www.loc.gov/METS/}techMD[@ID='mixmasterFile1']"
                     "//{http://www.loc.gov/mix/}ImageWidth"
                     ),
        'length':   (".//{http://www.loc.gov/METS/}techMD[@ID='mixmasterFile1']"
                     "//{http://www.loc.gov/mix/}ImageLength"
                     )
        }
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

        root = tree.getroot()
        m = XPATHMAP['issue']

        # get required metadata elements
        try:
            self.title = root.xpath('./@LABEL')[0]
            self.date = root.find(m['date']).text
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
        self.add_related(IssueMetadata(self.path,
            title='{0}, issue METS metadata'.format(self.title)
            ))
        self.add_related(IssueMetadata(self.article_path,
            title='{0}, article METS metadata'.format(self.title)
            ))

        # gather all the page and file xml snippets
        filexml_snippets = {
            elem.get('ID'): elem for elem in root.findall(m['files'])
            }
        pagexml_snippets = [p for p in root.findall(m['pages']) if \
            p.get('ID').startswith('pageModsBib')
            ]

        # iterate over each page section matching it to its files
        pages = {}
        premisxml = root.find(m['premis'])
        for n, pagexml in enumerate(pagexml_snippets):
            # attempt to match files
            pagenum = int(pagexml.get('ID').strip('pageModsBib'))
            try:
                filexml = filexml_snippets['pageFileGrp{0}'.format(pagenum)]
            except KeyError:
                filexml = None

            # create a page object for each page and append to list of pages
            page = Page(pagexml, filexml, premisxml, self)
            pages[str(pagenum)] = page
            self.add_component(page)

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
            textblocks = []
            for area in article.findall(m['areas']):
                pagenum = int(area.get('FILEID').replace('ocrFile', ''))
                page = pages[str(pagenum)]
                textblocks.append(page.ocr.textblock(area.get('BEGIN')))
                article_pagenums.add(pagenum)
            article = Article(article_title, self, pages=sorted(list(article_pagenums)))
            self.add_component(article)
            for textblock in textblocks:
                self.annotations.append(TextblockOnPage(textblock, page, article=article))

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

class TextblockOnPage(pcdm.Annotation):
    def __init__(self, textblock, page, article=None):
        super(TextblockOnPage, self).__init__()
        body = pcdm.TextualBody(textblock.text(), 'text/plain')
        if article is not None:
            body.linked_objects.append((dcterms.isPartOf, article))
        target = pcdm.SpecificResource(page)
        xywh = ','.join([ str(i) for i in textblock.xywh(page.ocr.scale) ])
        selector = pcdm.FragmentSelector(
            "xywh={0}".format(xywh),
            rdflib.URIRef('http://www.w3.org/TR/media-frags/')
            )
        xpath_selector = pcdm.XPathSelector("//*[@ID='{0}']".format(textblock.id))
        ocr_resource = pcdm.SpecificResource(page.ocr_file)
        ocr_resource.add_selector(xpath_selector)
        body.linked_objects.append((prov.wasDerivedFrom, ocr_resource))
        self.add_body(body)
        self.add_target(target)
        self.motivation = sc.painting
        target.add_selector(selector)
        self.fragments = [body, target, selector, ocr_resource, xpath_selector]

class IssueMetadata(pcdm.Component):
    '''additional metadata about an issue'''

    def __init__(self, file_path, title=None):
        super(IssueMetadata, self).__init__()
        file = MetadataFile(file_path)
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

    def __init__(self, localpath, title=None):
        super(MetadataFile, self).__init__(localpath, title)

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

    def __init__(self, pagexml, filegroup, premisxml, issue):
        super(Page, self).__init__()
        m = XPATHMAP['page']

        # gather metadata
        self.number    = pagexml.find(m['number']).text
        self.path      = issue.path + self.number
        self.reel      = pagexml.find(m['reel']).text
        if pagexml.find(m['frame']) is not None:
            self.frame = pagexml.find(m['frame']).text
        self.title     = "{0}, page {1}".format(issue.title, self.number)
        self.ordered   = True
        self.issue     = issue
        self.annotations = []

        # optionally generate a file object for each file in the XML snippet
        if filegroup is not None:
            for f in filegroup.findall(m['files']):
                file = File(f, issue.dir, premisxml)
                self.add_file(file)

            if file.use == 'ocr':
                # load ALTO XML into page object, for article text extraction
                try:
                    tree = ET.parse(file.localpath)
                except OSError as e:
                    raise DataReadException("Unable to read {0}".format(file.localpath))
                except ET.XMLSyntaxError as e:
                    raise DataReadException("Unable to parse {0} as XML".format(file.localpath))

                # TODO: read in resolution from issue METS data
                image_resolution = (400, 400)
                self.ocr_file = file
                self.ocr = ocr.ALTOResource(tree, image_resolution)

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

#============================================================================
# NDNP FILE OBJECT
#============================================================================

class File(pcdm.File):

    ''' class representing an individual file '''

    def __init__(self, filexml, dir, premisxml):
        self.use = filexml.get('USE')
        m = XPATHMAP['file']
        elem = filexml.find(m['filepath'])
        localpath = os.path.join(dir, os.path.basename(
            elem.get('{http://www.w3.org/1999/xlink}href')
            ))
        self.basename = os.path.basename(localpath)
        super(File, self).__init__(
            localpath, title="{0} ({1})".format(self.basename, self.use)
            )

        if self.basename.endswith('.tif'):
            self.width = premisxml.find(m['width']).text
            self.height = premisxml.find(m['length']).text
        else:
            self.width = None
            self.height = None

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
