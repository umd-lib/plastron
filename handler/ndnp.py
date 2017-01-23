''' Classes for interpreting and loading metadata and files stored
    according to the NDNP specification. '''

import csv
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
        'article':  (".//{http://www.loc.gov/mods/v3}title"
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

def load(args):
    return Batch(args)



#============================================================================
# NDNP BATCH CLASS
#============================================================================

class Batch():

    '''iterator class representing the set of resources to be loaded'''

    def __init__(self, args):
        tree = ET.parse(args.path)
        root = tree.getroot()
        m = XPATHMAP

        dback = Collection()
        dback.title = "The Diamondback Newspaper Collection"
        dback.graph.add(
            (dback.uri, dcterms.title, rdflib.Literal(dback.title))
            )
            
        self.collection = dback
        self.fieldnames = ['aggregation', 'sequence', 'uri']
        
        # read over the index XML file assembling a list of paths to the issues
        self.basepath = os.path.dirname(args.path)
        
        self.issues = []
        for i in root.findall(m['batch']['issues']):
            sanitized_path = i.text[:-6] + i.text[-4:]
            self.issues.append(
                (os.path.join(self.basepath, i.text),
                 os.path.join(
                    self.basepath, "Article-Level", sanitized_path)
                    )
                )
        
        # set up a CSV file for each reel
        self.reels = set([
            r.get('reelNumber') for r in root.findall(m['batch']['reels'])
            ])
        print('Batch contains {0} reels...'.format(len(self.reels)))
        for n, reel in enumerate(self.reels):
            filename = 'logs/{0}.csv'.format(reel)
            print("  {0}. Creating reel aggregation CSV in '{1}'...".format(n+1,
                                                                     filename))
            with open(filename, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
            
        self.length = len(self.issues)
        self.num = 0
        print("Batch contains {0} items.".format(self.length))


    def __iter__(self):
        return self


    def __next__(self):
        if self.num < self.length:
            issue = Issue(self.issues[self.num])
            issue.collections.append(self.collection)
            self.num += 1
            return issue
        else:
            print('\nProcessing complete!')
            raise StopIteration()



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
        self.graph.add(
            (self.uri, rdf.type, bibo.Issue)
            )

        # gather all the page and file xml snippets
        filexml_snippets = [f for f in root.findall(m['files'])]
        pagexml_snippets = [p for p in root.findall(m['pages']) if \
            p.get('ID').startswith('pageModsBib')
            ]

        # iterate over each page section matching it to its files
        premisxml = root.find(m['premis'])
        for n, pagexml in enumerate(pagexml_snippets):
            id = pagexml.get('ID').strip('pageModsBib')
            filexml = next(
                f for f in filexml_snippets if f.get('ID').endswith(id)
                )

            # create a page object for each page and append to list of pages
            page = Page(pagexml, filexml, premisxml, self)
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

    def __init__(self, csvfile):
        pcdm.Item.__init__(self)
        self.id = csvfile
        self.title = 'Reel Number {0}'.format(self.id)
        self.sequence_attr = ('Frame', 'frame')
        self.path = path
        self.components = pages

        self.graph.add(
            (self.uri, dcterms.title, rdflib.Literal(self.title))
            )
        self.graph.add(
            (self.uri, dc.identifier, rdflib.Literal(self.id))
            )
        self.graph.add(
            (self.uri, rdf.type, carriers.hd)
            )



#============================================================================
# NDNP PAGE OBJECT
#============================================================================

class Page(pcdm.Component):

    ''' class representing a newspaper page '''

    def __init__(self, pagexml, filegroup, premisxml, issue):
        pcdm.Component.__init__(self)
        m = XPATHMAP['page']

        # gather metadata
        self.number   = pagexml.find(m['number']).text
        self.path     = issue.path + self.number
        self.reel     = pagexml.find(m['reel']).text
        self.frame    = pagexml.find(m['frame']).text
        self.title    = "{0}, page {1}".format(issue.title, self.number)
        self.reelpath = 'logs/{0}.csv'.format(self.reel)

        # generate a file object for each file in the XML snippet
        for f in filegroup.findall(m['files']):
            self.files.append(File(f, issue.dir, premisxml))

        # store metadata in object graph
        self.graph.namespace_manager = namespace_manager
        self.graph.add( (self.uri, dcterms.title, rdflib.Literal(self.title)) )
        self.graph.add( (self.uri, ndnp.number, rdflib.Literal(self.number)) )
        self.graph.add( (self.uri, ndnp.sequence, rdflib.Literal(self.frame)) )
        self.graph.add( (self.uri, rdf.type, ndnp.Page) )

    
    # populate non-atomic aggregation object via overloaded superclass method
    def create_object(self, repository):
        if super(Page, self).create_object(repository):
            with open(self.reelpath, 'r') as f:
                fieldnames = f.readline().strip('\n').split(',')
            with open(self.reelpath, 'a+') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                row = {'aggregation':  self.reel,
                       'sequence':     self.frame,
                       'uri':          self.uri
                        }
                writer.writerow(row)



#============================================================================
# NDNP FILE OBJECT
#============================================================================

class File(pcdm.File):

    ''' class representing an individual file '''

    def __init__(self, filexml, dir, premisxml):
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
        self.graph.add( (self.uri, dcterms.type, dcmitype.Text) )

        if self.basename.endswith('.tif'):
            self.width = premisxml.find(m['width']).text
            self.height = premisxml.find(m['length']).text
            self.graph.add(
                (self.uri, ebucore.width, rdflib.Literal(self.width))
                )
            self.graph.add(
                (self.uri, ebucore.height, rdflib.Literal(self.height))
                )
            self.graph.add(
                (self.uri, rdf.type, pcdm_use.PreservationMasterFile)
                )
        elif self.basename.endswith('.jp2'):
            self.graph.add(
                (self.uri, rdf.type, pcdm_use.IntermediateFile)
                )
        elif self.basename.endswith('.pdf'):
            self.graph.add(
                (self.uri, rdf.type, pcdm_use.ServiceFile)
                )
        elif self.basename.endswith('.xml'):
            self.graph.add(
                (self.uri, rdf.type, pcdm_use.ExtractedText)
                )



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
        self.graph.add( (self.uri, dcterms.title, rdflib.Literal(self.title)) )
        self.graph.add( (self.uri, rdf.type, bibo.Article) )



