''' Classes for interpreting and loading metadata and files stored
    according to the NDNP specification. '''

import lxml.etree as ET
import os
from classes import pcdm
import rdflib
from rdflib import Namespace, URIRef



#============================================================================
# NAMESPACE BINDINGS
#============================================================================

namespace_manager = rdflib.namespace.NamespaceManager(rdflib.Graph())

bibo = Namespace('http://purl.org/ontology/bibo/')
namespace_manager.bind('bibo', bibo, override=False)

dc = Namespace('http://purl.org/dc/elements/1.1/')
namespace_manager.bind('dc', dc, override=False)

foaf = Namespace('http://xmlns.com/foaf/0.1/')
namespace_manager.bind('foaf', foaf, override=False)

ore = Namespace('http://www.openarchives.org/ore/terms/')
namespace_manager.bind('ore', ore, override=False)

iana = Namespace('http://www.iana.org/assignments/relation/')
namespace_manager.bind('iana', iana, override=False)

ex = Namespace('http://www.example.org/terms/')
namespace_manager.bind('ex', ex, override=False)

pcdm_ns = Namespace('http://pcdm.org/models#')
namespace_manager.bind('pcdm', pcdm_ns, override=False)

rdf = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
namespace_manager.bind('rdf', rdf, override=False)



#============================================================================
# METADATA MAPPING
#============================================================================

XPATHMAP = {
    'batch': {
        'issues':   "./{http://www.loc.gov/ndnp}issue"
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
        },

    'article': {
        'title':    (".//{http://www.loc.gov/mods/v3}title"
                    )
        }
    }



#============================================================================
# DATA LOADING FUNCTION
#============================================================================

def load(path_to_batch_xml):
    return Batch(path_to_batch_xml)



#============================================================================
# NDNP BATCH CLASS
#============================================================================

class Batch():

    '''class representing the set of resources to be loaded'''

    def __init__(self, batchfile):
        tree = ET.parse(batchfile)
        root = tree.getroot()
        m = XPATHMAP

        self.reel = Reel(batchfile)

        dback = Collection()
        dback.title = "The Diamondback Newspaper Collection"
        dback.graph.add(
            (dback.uri, dc.title, rdflib.Literal(dback.title))
            )

        # read over the index XML file assembling a list of paths to the issues
        self.basepath = os.path.dirname(batchfile)
        self.paths = [
            os.path.join(self.basepath,
                i.text) for i in root.findall(m['batch']['issues'])
            ]
        self.length = len(self.paths)

        print("Batch contains {} issues.".format(self.length))

        self.items = []

        # iterate over the paths to the issues and create an item from each one
        for n, p in enumerate(self.paths):
            print("Preprocessing item {0}/{1}...".format(n+1,
                self.length), end='\r')

            if not os.path.isfile(p):
                print("\nMissing item {0}, skipping".format(n+1))
                continue

            issue = Issue(p)
            self.items.append(issue)

            # add the collection to the issue
            issue.collections.append(dback)

            # add issue's pages to the reel object
            for page in issue.components:
                self.reel.components.append(page)

        self.items.append(self.reel)

        # iterate over the article-level XML and get articles
        articlexmlpath = os.path.dirname(batchfile) + "Article-Level"
        articlefiles = []
        for root, dirs, files in os.walk(articlexmlpath):
            for file in files:
                articlefiles.append(os.path.join(root, file))

        articles = {}
        for file in articlefiles:
            tree = ET.parse(file)
            root = tree.getroot()
            articles[os.path.basename(file)] = [
                art for art in root.findall(m['article']['title'])
                ]


        print('\nPreprocessing complete!')


    # print the hierarchy of items, pages, and files
    def print_tree(self):
        for n, i in enumerate(self.items):
            print("\nITEM {0}".format(n+1))
            i.print_item_tree()
        print('')



#============================================================================
# NDNP ISSUE OBJECT
#============================================================================

class Issue(pcdm.Item):

    ''' class representing all components of a newspaper issue '''

    def __init__(self, path):
        pcdm.Item.__init__(self)
        tree = ET.parse(path)
        root = tree.getroot()
        m = XPATHMAP['issue']

        # gather metadata
        self.dir        = os.path.dirname(path)
        self.path           = path
        self.title          = root.xpath('./@LABEL')[0]
        self.volume         = root.find(m['volume']).text
        self.issue          = root.find(m['issue']).text
        self.edition        = root.find(m['edition']).text
        self.date           = root.find(m['date']).text
        self.sequence_attr  = ('Page', 'number')

        # store metadata as an RDF graph
        self.graph.namespace_manager = namespace_manager
        self.graph.add(
            (self.uri, dc.title, rdflib.Literal(self.title))
            )
        self.graph.add(
            (self.uri, bibo.volume, rdflib.Literal(self.volume))
            )
        self.graph.add(
            (self.uri, bibo.issue, rdflib.Literal(self.issue))
            )
        self.graph.add(
            (self.uri, bibo.edition, rdflib.Literal(self.edition))
            )
        self.graph.add(
            (self.uri, dc.date, rdflib.Literal(self.date))
            )

        # gather all the page and file xml snippets
        filexml_snippets = [f for f in root.findall(m['files'])]
        pagexml_snippets = [p for p in root.findall(m['pages']) if \
            p.get('ID').startswith('pageModsBib')
            ]

        # iterate over each page section matching it to its files
        for n, pagexml in enumerate(pagexml_snippets):
            id = pagexml.get('ID').strip('pageModsBib')
            filexml = next(
                f for f in filexml_snippets if f.get('ID').endswith(id)
                )

            # create a page object for each page and append to list of pages
            page = Page(pagexml, filexml, self)

            self.components.append(page)



#============================================================================
# NDNP REEL OBJECT
#============================================================================

class Reel(pcdm.Item):

    ''' class representing an NDNP reel '''

    def __init__(self, batchfile):
        pcdm.Item.__init__(self)
        tree = ET.parse(batchfile)
        root = tree.getroot()
        m = XPATHMAP['reel']
        elem = root.find(m['number'])
        self.id = elem.get('reelNumber')
        self.title = 'Reel Number {0}'.format(self.id)
        self.sequence_attr = ('Frame', 'frame')

        self.graph.add(
            (self.uri, dc.title, rdflib.Literal(self.title))
            )
        self.graph.add(
            (self.uri, dc.identifier, rdflib.Literal(self.id))
            )



#============================================================================
# NDNP PAGE OBJECT
#============================================================================

class Page(pcdm.Component):

    ''' class representing a newspaper page '''

    def __init__(self, pagexml, filegroup, issue):
        pcdm.Component.__init__(self)
        m = XPATHMAP['page']

        # gather metadata
        self.number = pagexml.find(m['number']).text
        self.path   = issue.path + self.number
        self.reel   = pagexml.find(m['reel']).text
        self.frame  = pagexml.find(m['frame']).text
        self.title  = "{0}, page {1}".format(issue.title, self.number)

        # generate a file object for each file in the XML snippet
        for f in filegroup.findall(m['files']):
            self.files.append(File(f, issue.dir))

        # store metadata in object graph
        self.graph.namespace_manager = namespace_manager
        self.graph.add( (self.uri, dc.title, rdflib.Literal(self.title)) )
        self.graph.add( (self.uri, ex.reel, rdflib.Literal(self.reel)) )
        self.graph.add( (self.uri, ex.frame, rdflib.Literal(self.frame)) )



#============================================================================
# NDNP FILE OBJECT
#============================================================================

class File(pcdm.File):

    ''' class representing an individual file '''

    def __init__(self, filexml, dir):
        m = XPATHMAP['file']
        elem = filexml.find(m['filepath'])
        pcdm.File.__init__(self, os.path.join(dir, os.path.basename(
                            elem.get('{http://www.w3.org/1999/xlink}href')
                            )))
        self.basename = os.path.basename(self.localpath)
        self.use  = filexml.get('USE')
        self.title = "{0} ({1})".format(self.basename, self.use)

        # store metadata in object graph
        self.graph.namespace_manager = namespace_manager
        self.graph.add( (self.uri, dc.title, rdflib.Literal(self.title)) )



#============================================================================
# NDNP COLLECTION OBJECT
#============================================================================

class Collection(pcdm.Collection):

    ''' class representing a collection of newspaper resources '''

    def __init__(self):
        pcdm.Collection.__init__(self)




#============================================================================
# NDNP ARTICLE OBJECT
#============================================================================

class Article(pcdm.Item):

    ''' class representing an article in a newspaper issue '''

    def __init__(self, filexml):
        pass


