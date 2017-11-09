'''handler for loading reel objects created by the ndnp handler'''

import csv
import logging
import os
from rdflib import Graph, Literal, URIRef
from classes import pcdm
from classes.exceptions import ConfigException
from namespaces import carriers, dcterms, rdf

#============================================================================
# DATA LOADING FUNCTION
#============================================================================

def load(repo, batch_config):
    return Batch(repo, batch_config)

#============================================================================
# CSV BATCH CLASS
#============================================================================

class Batch():

    '''iterator class representing the set of resources to be loaded'''

    def __init__(self, repo, batch_config):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )
        self.collection = pcdm.Collection()
        self.collection.uri = URIRef(batch_config.get('COLLECTION'))

        # check that the supplied collection exists and get title
        response = repo.get(
            self.collection.uri, headers={'Accept': 'application/rdf+xml'}
            )
        if response.status_code == 200:
            coll_graph = Graph().parse(data=response.text)
            self.collection.title = str(self.collection.uri)
            for (subj, pred, obj) in coll_graph:
                if str(pred) == "http://purl.org/dc/elements/1.1/title":
                    self.collection.title = obj
        else:
            raise ConfigException(
                "Collection URI {0} could not be reached.".format(
                    self.collection.uri
                    )
                )
        self.collections = [self.collection]
        self.path = batch_config.get('LOCAL_PATH')
        self.files = [os.path.join(self.path, f) for f in os.listdir(self.path)]
        self.length = len(self.files)
        self.num = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.num < self.length:
            reel = Reel(self.files[self.num])
            reel.collections = self.collections
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
        super(Reel, self).__init__()
        self.id = os.path.splitext(os.path.basename(csvfile))[0]
        self.title = 'Reel Number {0}'.format(self.id)
        self.sequence_attr = ('Frame', 'sequence')
        self.path = csvfile

        with open(self.path, 'r') as f:
            reader = csv.DictReader(f)
            self.components = [
                Frame(self, row['sequence'], row['uri']) for row in reader
                ]

        self.graph.add(
            (self.uri, dcterms.title, Literal(self.title)))
        self.graph.add(
            (self.uri, dcterms.identifier, Literal(self.id)))
        self.graph.add(
            (self.uri, rdf.type, carriers.hd))

#============================================================================
# NDNP FRAME OBJECT
#============================================================================

class Frame(pcdm.Component):

    '''class referencing an existing page object for purpose of reel creation'''

    def __init__(self, reel, sequence, uri):
        super(Frame, self).__init__()

        self.sequence = sequence
        self.uri = URIRef(uri)

        self.title = "{0}, frame {1}".format(reel.title, self.sequence)
        self.ordered = True
