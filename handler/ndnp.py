'''Classes for interpreting and loading metadata and files stored 
according to the NDNP specification.'''

import lxml.etree as ET
import os
from classes import pcdm
import rdflib
from rdflib import Namespace

bibo    = Namespace('http://purl.org/ontology/bibo/')
dc      = Namespace('http://purl.org/dc/elements/1.1/')
foaf    = Namespace('http://xmlns.com/foaf/0.1/')
premis  = Namespace('http://www.loc.gov/premis/rdf/v1#')
image   = Namespace('<http://www.modeshape.org/images/1.0')
sv      = Namespace('http://www.jcp.org/jcr/sv/1.0')
nt      = Namespace('http://www.jcp.org/jcr/nt/1.0')
rdfs    = Namespace('http://www.w3.org/2000/01/rdf-schema#')
xsi     = Namespace('http://www.w3.org/2001/XMLSchema-instance')
mode    = Namespace('http://www.modeshape.org/1.0')
xmlns   = Namespace('http://www.w3.org/2000/xmlns/')
rdf     = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
xml     = Namespace('http://www.w3.org/XML/1998 Namespace')
jcr     = Namespace('http://www.jcp.org/jcr/1.0')
ebucore = Namespace('http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#')
ldp     = Namespace('<http://www.w3.org/ns/ldp#')
xs      = Namespace('<http://www.w3.org/2001/XMLSchema')
mix     = Namespace('<http://www.jcp.org/jcr/mix/1.0')
prov    = Namespace('<http://www.w3.org/ns/prov#')

namespace_manager = rdflib.namespace.NamespaceManager(rdflib.Graph())
namespace_manager.bind('bibo', bibo, override=False)
namespace_manager.bind('dc', dc, override=False)
namespace_manager.bind('foaf', foaf, override=False)


#============================================================================
# METADATA MAPPING
#============================================================================

METAMAP = {
    'batch': {
        'issues':   "./{http://www.loc.gov/ndnp}issue"
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
        }
    }


#============================================================================
# CLASSES
#============================================================================

class batch(pcdm.Resource):

    '''class representing the set of resources to be loaded'''

    def __init__(self, batchfile):
        tree = ET.parse(batchfile)
        root = tree.getroot()
        m = METAMAP['batch']
        
        # read over the index XML file assembling a list of paths to the issues
        self.basepath = os.path.dirname(batchfile)
        self.paths = [
            os.path.join(self.basepath, 
                i.text) for i in root.findall(m['issues'])
            ]
        self.num_items = len(self.paths)
        
        print("Batch contains {} issues.".format(len(self.paths)))
        
        self.items = []
        
        # iterate over the paths to the issues and create an item from each one
        for n, p in enumerate(self.paths):
            print("Processing item {0}/{1}...".format(n+1, 
                    self.num_items), end='\r')
            self.items.append(item(p))
        
        print('\nDone!')
        
        for n, i in enumerate(self.items):
            print('\n{0}. {1}'.format(n, i))
            for p in i.pages:
                print(" |--", p)
                for f in p.files:
                    print(" | |--", f)

    # print the hierarchy of items, pages, and files
    def print_tree(self):
        for n, i in enumerate(self.items):
            print("\n  {0}. {1}".format(n + 1, i.title))
            for p_num, p in enumerate(i.pages):
                print("      p{0}: {1}".format(p_num + 1, p.title))
                for f in p.files:
                    print("          |--{0}".format(f.title))
        print('')



class item(pcdm.Resource):

    '''class representing all components of an individual item'''

    def __init__(self, path):
        tree = ET.parse(path)
        root = tree.getroot()
        m = METAMAP['issue']
        
        # gather metadata
        self.dir     = os.path.dirname(path)
        self.title   = root.xpath('./@LABEL')[0]
        self.volume  = root.find(m['volume']).text
        self.issue   = root.find(m['issue']).text
        self.edition = root.find(m['edition']).text
        self.date    = root.find(m['date']).text
        self.pages   = []
        
        # store metadata as an RDF graph
        self.id      = rdflib.BNode()
        self.graph   = rdflib.Graph()
        self.graph.namespace_manager = namespace_manager
        
        self.graph.add( (self.id, dc.title, rdflib.Literal(self.title)) )
        self.graph.add( (self.id, bibo.volume, rdflib.Literal(self.volume)) )
        self.graph.add( (self.id, bibo.issue, rdflib.Literal(self.issue)) )
        self.graph.add( (self.id, bibo.edition, rdflib.Literal(self.edition)) )
        self.graph.add( (self.id, dc.date, rdflib.Literal(self.date)) )
        
        # print the graph (uncomment to debug)
        # print(self.graph.serialize(format='turtle'))
        
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
            p = page(pagexml, filexml, self)
            self.pages.append(p)
            
            # iterate over the files for each page, and set the path
            for f in p.files:
                f.path = os.path.join(self.dir, os.path.basename(f.relpath))
                
    

class page(pcdm.Resource):

    '''class representing the individual page'''

    def __init__(self, pagexml, filegroup, issue):
        m = METAMAP['page']
        
        # gather metadata
        self.number = pagexml.find(m['number']).text
        self.reel   = pagexml.find(m['reel']).text
        self.frame  = pagexml.find(m['frame']).text
        self.title  = "{0}, page {1}".format(issue.title, self.number)
        
        # generate a file object for each file in the XML snippet
        self.files  = [file(f) for f in filegroup.findall(m['files'])]
        
        # store metadata as an RDF graph
        self.id      = rdflib.BNode()
        self.graph   = rdflib.Graph()
        self.graph.namespace_manager = namespace_manager
        
        self.graph.add( (self.id, dc.title, rdflib.Literal(self.title)) )
        self.graph.add( (self.id, dc.reel, rdflib.Literal(self.reel)) )
        self.graph.add( (self.id, dc.frame, rdflib.Literal(self.frame)) )
        

        
class file():
    '''class representing the individual file'''
    def __init__(self, filexml):
        m = METAMAP['file']
        self.use  = filexml.get('USE')
        elem = filexml.find(m['filepath'])
        self.relpath = elem.get('{http://www.w3.org/1999/xlink}href')
        self.basename = os.path.basename(self.relpath)
        self.title = "{0} ({1})".format(self.basename, self.use)
        
        # store metadata as an RDF graph
        self.id      = rdflib.BNode()
        self.graph   = rdflib.Graph()
        self.graph.namespace_manager = namespace_manager
        
        self.graph.add( (self.id, dc.title, rdflib.Literal(self.title)) )
        
        
#============================================================================
# DATA LOADING FUNCTION
#============================================================================

def load(path_to_batch_xml):
    return batch(path_to_batch_xml)
    
