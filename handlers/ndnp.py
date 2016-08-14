'''Classes for interpreting and loading metadata and files stored 
according to the NDNP specification.'''

import lxml.etree as ET
import os

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


def data_handler(path_to_batch_xml):
    b = batch(path_to_batch_xml)


#============================================================================
# CLASSES
#============================================================================

class batch():
    '''class representing the set of resources to be loaded'''
    def __init__(self, batchfile):
        tree = ET.parse(batchfile)
        root = tree.getroot()
        m = METAMAP['batch']
        
        self.basepath = os.path.dirname(batchfile)
        self.paths = [
            os.path.join(self.basepath, 
                i.text) for i in root.findall(m['issues'])
            ]
        self.num_items = len(self.paths)
        
        print("Batch contains {} issues.".format(len(self.paths)))
        
        self.items = []
        for n, p in enumerate(self.paths):
            print("Processing item {0}/{1}...".format(n+1, 
                    self.num_items), end='\r')
            self.items.append(item(p))
        
        print('\nDone!')


class item():
    '''class representing the pieces of an individual item'''
    def __init__(self, path):
        tree = ET.parse(path)
        root = tree.getroot()
        m = METAMAP['issue']
        
        self.dir     = os.path.dirname(path)
        self.volume  = root.find(m['volume']).text
        self.issue   = root.find(m['issue']).text
        self.edition = root.find(m['edition']).text
        self.date    = root.find(m['date']).text
        self.pages   = []
        
        filexml_snippets = [f for f in root.findall(m['files'])]
        pagexml_snippets = [p for p in root.findall(m['pages']) if \
            p.get('ID').startswith('pageModsBib')
            ]
        
        for n, pagexml in enumerate(pagexml_snippets):
            id = pagexml.get('ID').strip('pageModsBib')
            filexml = next(
                f for f in filexml_snippets if f.get('ID').endswith(id)
                )
            p = page(pagexml, filexml)
            self.pages.append(p)
            for f in p.files:
                f.path = os.path.join(self.dir, os.path.basename(f.relpath))


class page():
    '''class representing the individual page'''
    def __init__(self, pagexml, filegroup):
        m = METAMAP['page']
        self.reel   = pagexml.find(m['reel']).text
        self.frame  = pagexml.find(m['frame']).text
        self.files  = [file(f) for f in filegroup.findall(m['files'])]
        
        
class file():
    '''class representing the individual file'''
    def __init__(self, filexml):
        m = METAMAP['file']
        self.use  = filexml.get('USE')
        elem = filexml.find(m['filepath'])
        self.relpath = elem.get('{http://www.w3.org/1999/xlink}href')
        
