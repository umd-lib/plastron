'''handler for loading reel objects created by the ndnp handler'''

import csv
import logging
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
# DATA LOADING FUNCTION
#============================================================================

def load(args):
    return Batch(args)

#============================================================================
# CSV BATCH CLASS
#============================================================================

class Batch():

    '''iterator class representing the set of resources to be loaded'''

    def __init__(self, args):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )

        self.collection = 'http://localhost:8080/rest/foo'
        self.files = []
        
        for f in os.listdir(args.path):
            print(os.
            
        print(self.files)
                
        self.length = len(self.files)
        self.num = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.num < self.length:
            reel = Reel(self.files[self.num])
            self.num += 1
            return reel
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()

#============================================================================
# NDNP REEL OBJECT
#============================================================================

class Reel(pcdm.Item):

    '''class representing an NDNP reel'''

    def __init__(self, csvfile):
        pcdm.Item.__init__(self)
        self.id = os.path.splitext(os.path.basename(csvfile))[0]
        self.title = 'Reel Number {0}'.format(self.id)
        self.sequence_attr = ('Frame', 'frame')
        self.path = csvfile
        
        with open(self.path, 'r') as f:
            reader = csv.DictReader(f)
            self.components = [(row['sequence'], row['uri']) for row in reader]

        self.graph.add(
            (self.uri, dcterms.title, rdflib.Literal(self.title))
            )
        self.graph.add(
            (self.uri, dc.identifier, rdflib.Literal(self.id))
            )
        self.graph.add(
            (self.uri, rdf.type, carriers.hd)
            )

        print(reel)
