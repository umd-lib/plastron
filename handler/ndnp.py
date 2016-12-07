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

dcterms = Namespace('http://purl.org/dc/terms/')
namespace_manager.bind('dcterms', dcterms, override=False)

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
        'article':  (".//{http://www.loc.gov/mods/v3}title"
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
        }
    }



#============================================================================
# DATA LOADING FUNCTION
#============================================================================

def load(args):
    return Batch(args.path, args.limit)



#============================================================================
# NDNP BATCH CLASS
#============================================================================

class Batch():

    '''class representing the set of resources to be loaded'''

    def __init__(self, batchfile, limit):
        tree = ET.parse(batchfile)
        root = tree.getroot()
        m = XPATHMAP

        dback = Collection()
        dback.title = "The Diamondback Newspaper Collection"
        dback.graph.add(
            (dback.uri, dcterms.title, rdflib.Literal(dback.title))
            )

        # read over the index XML file assembling a list of paths to the issues
        self.basepath = os.path.dirname(batchfile)
        self.paths = []
        for i in root.findall(m['batch']['issues']):
            sanitized_path = i.text[:-6] + i.text[-4:]
            self.paths.append(
                (os.path.join(self.basepath, i.text),
                 os.path.join(
                    self.basepath, "Article-Level", sanitized_path)
                    )
                )
        self.length = len(self.paths)
        
        self.reel = Reel(batchfile)

        print("Batch contains {} issues.".format(self.length))

        self.items = []

        # iterate over the paths to the issues and create an item from each one
        for n, p in enumerate(self.paths):
            print("Preprocessing item {0}/{1}...".format(
                n+1, self.length), end='\r'
                )
            
            if limit is not None and len(self.items) >= limit:
                print("Stopping preprocessing after {0} items".format(limit))
                break
            
            if not os.path.isfile(p[0]) or not os.path.isfile(p[1]):
                print("\nMissing file for item {0}, skipping".format(n+1))
                continue

            issue = Issue(p)
            self.items.append(issue)

            # add the collection to the issue
            issue.collections.append(dback)

            # add issue's pages to the reel object
            for page in issue.components:
                self.reel.components.append(page)

        self.items.append(self.reel)

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

    def __init__(self, paths):
        (issue_path, article_path) = paths
        pcdm.Item.__init__(self)
        tree = ET.parse(issue_path)
        root = tree.getroot()
        m = XPATHMAP['issue']

        # gather metadata
        self.dir            = os.path.dirname(issue_path)
        self.path           = issue_path
        self.title          = root.xpath('./@LABEL')[0]
        self.volume         = root.find(m['volume']).text
        self.issue          = root.find(m['issue']).text
        self.edition        = root.find(m['edition']).text
        self.date           = root.find(m['date']).text
        self.sequence_attr  = ('Page', 'number')

        # store metadata as an RDF graph
        self.graph.namespace_manager = namespace_manager
        self.graph.add(
            (self.uri, dcterms.title, rdflib.Literal(self.title))
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
            
        # iterate over the article XML and create objects for issue's articles
        article_tree = ET.parse(article_path)
        article_root = article_tree.getroot()
        for article_title in article_root.findall(m['article']):
            self.related.append(Article(article_title.text, self))



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
        self.basepath = os.path.dirname(batchfile)
        self.path = os.path.join(self.basepath, elem.text)
        print("Reel path = ", self.path)

        self.graph.add(
            (self.uri, dcterms.title, rdflib.Literal(self.title))
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
        self.graph.add( (self.uri, dcterms.title, rdflib.Literal(self.title)) )
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
        self.graph.add( (self.uri, dcterms.title, rdflib.Literal(self.title)) )



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

    def __init__(self, title, issue):
        pcdm.Item.__init__(self)

        # gather metadata
        self.title = title

        # store metadata in object graph
        self.graph.namespace_manager = namespace_manager
        self.graph.add( (self.uri, dc.title, rdflib.Literal(self.title)) )
        
        print("Creating Article object {0}".format(self.title))
        

