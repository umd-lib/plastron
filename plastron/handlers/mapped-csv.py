import csv
import logging
import os
import sys
import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.util import from_n3
from plastron import pcdm, ldp, namespaces
from plastron.exceptions import ConfigException, DataReadException
from plastron.namespaces import dcmitype, dcterms, pcdmuse, rdf

#============================================================================
# DATA LOADING FUNCTION
#============================================================================

def load(repo, batch_config):
    return Batch(repo, batch_config)

#============================================================================
# BATCH CLASS (FOR BINARIES PLUS CSV METADATA)
#============================================================================

class Batch():
    '''Class representing the mapped and parsed CSV data'''
    def __init__(self, repo, config):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        # Check for required configuration items and set up paths
        required_keys = ['HANDLER',
                         'COLLECTION',
                         'ROOT',
                         'MAPFILE',
                         'METADATA_FILE',
                         'METADATA_MAP',
                         'DATA_PATH'
                         ]
        for key in required_keys:
            if not config.get(key):
                raise ConfigException(
                    'Missing required key {0} in batch config'.format(key))

        # Set configuration properties
        self.root          = os.path.normpath(config.get('ROOT'))
        self.data_path     = os.path.join(self.root, config['DATA_PATH'])
        self.metadata_file = os.path.join(
                                self.root, config.get('METADATA_FILE')
                                )
        self.collection = pcdm.Collection.from_repository(
                                repo, config.get('COLLECTION')
                                )
        self.metadata_map = os.path.join(
                                self.root, config.get('METADATA_MAP')
                                )

        # Check for required files and parse them
        required_files = ['metadata_file', 'metadata_map']
        for rf in required_files:
            if not os.path.isfile(getattr(self, rf)):
                raise ConfigException('{} could not be found'.format(rf))
        with open(self.metadata_map, 'r') as f:
            self.logger.info(
                'Parsing the metadata map in {0}'.format(self.metadata_map)
                )
            self.mapping = yaml.safe_load(f)
        with open(self.metadata_file, 'r') as f:
            self.rows = [r for r in csv.DictReader(f)]
        self.length = len(self.rows)
        self.logger.info('Batch contains {0} items.'.format(self.length))
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.count < self.length:
            row = self.rows[self.count]
            fnames = row.pop('files').split(self.mapping['files']['separator'])
            file_list = [os.path.join(self.data_path, f) for f in fnames]
            item = Item(row, self.mapping, files=file_list)
            item.add_collection(self.collection)
            item.path = '{}, item {}'.format(self.metadata_file, self.count)
            self.count += 1
            return item
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()


#============================================================================
# ITEM CLASS
#============================================================================

class Item(pcdm.Item):
    '''Class representing a self-contained repository resource'''
    def __init__(self, data, map, title=None, files=None, parts=None):
        super(Item, self).__init__()
        self.src_graph = Graph()
        self.data = data
        self.map = map
        self.title = data['title']
        self.files = files

    def read_data(self):
        nsm = namespaces.get_manager()
        for key, value in self.data.items():
            if key in self.map:
                mapping = self.map[key]
                pred_uri = from_n3(mapping['predicate'], nsm=nsm)
                if mapping.get('uriref', False):
                    o = URIRef(value)
                else:
                    datatype = mapping.get('datatype', None)
                    if datatype is not None:
                        datatype_uri = from_n3(datatype, nsm=nsm)
                        o = Literal(value, datatype=datatype_uri)
                    else:
                        o = Literal(value)
                self.src_graph.add((self.uri, pred_uri, o))
            else:
                pass
        for f in self.files:
            self.add_file(File.from_localpath(f))

    def graph(self):
        graph = super(Item, self).graph()
        if self.src_graph is not None:
            for (s, p, o) in self.src_graph:
                graph.add((self.uri, p, o))
        return graph


#============================================================================
# FILE CLASS
#============================================================================

class File(pcdm.File):
    '''Class representing file associated with an item resource'''
    def graph(self):
        graph = super(File, self).graph()
        graph.add((self.uri, dcterms.title, Literal(self.title)))
        graph.add((self.uri, dcterms.type, dcmitype.Text))
        if self.filename.endswith('.tif'):
            graph.add((self.uri, rdf.type, pcdmuse.PreservationMasterFile))
        elif self.filename.endswith('.jpg'):
            graph.add((self.uri, rdf.type, pcdmuse.IntermediateFile))
        elif self.filename.endswith('.xml'):
            graph.add((self.uri, rdf.type, pcdmuse.ExtractedText))
        elif self.filename.endswith('.txt'):
            graph.add((self.uri, rdf.type, pcdmuse.ExtractedText))
        return graph
