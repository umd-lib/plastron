''' A handler for loading sequenced assets from binaries & turtle metadata. '''

import logging
import os
import re
import sys
import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from classes import pcdm
from classes.exceptions import ConfigException, DataReadException
from namespaces import bibo, dc, dcmitype, dcterms, fabio, pcdmuse, rdf

#============================================================================
# DATA LOADING FUNCTION
#============================================================================

def load(repo, batch_config):
    return Batch(repo, batch_config)


#============================================================================
# BATCH CLASS (FOR PAGED BINARIES PLUS RDF METADATA)
#============================================================================

class Batch():

    '''Iterator class representing a set of resources to be loaded'''

    def __init__(self, repo, config):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )

        # Check for required configuration items and set up paths
        required_keys = ['HANDLER',
                         'COLLECTION',
                         'ROOT',
                         'MAPFILE',
                         'BATCH_INDEX',
                         'LOG_LOCATION',
                         'LOG_CONFIG',
                         'METADATA_FILE',
                         'METADATA_PATH',
                         'DATA_PATH'
                         ] 
        for key in required_keys:
            if not config.get(key):
                raise ConfigException(
                    'Missing required key {0} in batch config'.format(key)
                    )

        # Set configuration Properties
        self.local_path    = os.path.normpath(config.get('ROOT'))
        self.index_file    = os.path.join(self.local_path,
                                          config.get('BATCH_INDEX'))
        self.data_path     = os.path.join(self.local_path, 
                                          config['DATA_PATH'])
        self.metadata_path = os.path.join(self.local_path, 
                                          config['METADATA_PATH'])
        self.metadata_file = os.path.join(self.local_path,
                                          config.get('METADATA_FILE'))
        self.collection    = pcdm.Collection.from_repository(repo,
                                                     config.get('COLLECTION'))

        # Create data structures to accumulate process results
        self.items       = {}
        self.incomplete  = []
        self.extra_files = []

        # Check for required metadata file and path
        if not os.path.isdir(self.metadata_path):
            os.mkdir(self.metadata_path)
        if not os.path.isfile(self.metadata_file):
            raise ConfigException('Specified metadata file could not be found')

        # Generate index of all files in the data path
        self.logger.info('Walking the "data path" tree to create a file index')
        self.all_files = {}
        for root, dirs, files in os.walk(self.data_path):
            for f in files:
                self.all_files[f] = os.path.join(root,f)
        self.logger.info("Found {0} files".format(len(self.all_files)))

        # Generate item-level metadata graphs and store as files
        with open(self.metadata_file, 'r') as f:
            self.logger.info('Parsing the master metadata graph')
            g = Graph().parse(f, format="turtle")
            # For each of the unique subjects in the graph
            for subj_uri in set([i for i in g.subjects()]):
                # Get the item identifier
                itembase = os.path.basename(subj_uri)
                # Create the path to the output file
                outfile = os.path.join(self.metadata_path, itembase) + '.ttl'
                # Create a graph of all triples with that subject
                itemgraph = Graph()
                itemgraph += g.triples((subj_uri, None, None))
                # Serialize the graph to the path location
                self.logger.info('Serializing graph {0}'.format(outfile))
                itemgraph.serialize(destination=outfile, format="turtle")

        # If available, read batch index from file
        if os.path.isfile(self.index_file):
            self.logger.info("Reading batch index from {0}".format(self.index_file))
            with open(self.index_file, 'r') as infile:
                self.items = yaml.load(infile)

        # Otherwise, construct the index by reading graph files
        else:
            for f in os.listdir(self.metadata_path):
                fullpath = os.path.join(self.metadata_path, f)
                # skip files starting with dot
                if f.startswith('.'):
                    self.extra_files.append(fullpath)
                    continue
                else:
                    try:
                        id = os.path.basename(f).rstrip('.ttl')
                        self.items[id] = self.create_item(fullpath)
                    except:
                        self.incomplete.append(fullpath)

            for id in self.items:
                print('=' * 65)
                print(id.upper())
                print('FILES:', self.items[id]['files'])
                print('PARTS:', self.items[id]['parts'])
                print('METADATA:', self.items[id]['metadata'])

            # Serialize the index to a YAML file
            self.logger.info("Serializing index to {0}".format(
                                                        self.index_file))
            with open(self.index_file, 'w') as outfile:
                yaml.dump(self.items, outfile, default_flow_style=False)

        # Create list of complete item keys and set up counters
        self.to_load = sorted(self.items.keys())
        self.length = len(self.to_load)
        self.count = 0
        self.logger.info("Batch contains {0} items.".format(self.length))

    def create_item(self, item_metadata):
        data = {'metadata':  os.path.relpath(item_metadata, self.local_path),
                'parts': {},
                'files': []
                }
        item_graph = Graph().parse(item_metadata, format="turtle")
        subject    = next(item_graph.subjects())
        extent     = int(item_graph.value(subject, dcterms.extent).split(' ')[0])
        parts      = {}

        # Parse each filename in hasPart and allocate to correct location in item entry
        for filename in [str(f) for f in item_graph.objects(predicate=dcterms.hasPart)]:
            normalized = filename.replace('_', '-')
            basename, ext = os.path.splitext(normalized)
            base_parts = basename.split('-')
            # handle files with no sequence id
            if len(base_parts) == 2:
                data['files'].append(self.all_files[filename])
            # handle files with a sequence id
            elif len(base_parts) == 3:
                page_no = str(int(base_parts[2]))
                if page_no not in parts:
                    parts[page_no] = {'files': [self.all_files[filename]], 'parts': {}}
                else:
                    parts[page_no]['files'].append(self.all_files[filename])
            else:
                print("ERROR!")

        # Add items in parts dict to index entry according to position in sequence
        for n, p in enumerate(sorted(parts.keys())):
            data['parts'][n+1] = parts[p]

        return data

    def __iter__(self):
        return self

    def __next__(self):
        if self.count < self.length:
            id = self.to_load[self.count]
            item_map = self.items[id]
            item = Item(id, item_map, self.local_path)
            item.add_collection(self.collection)
            self.count += 1
            return item
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()


#============================================================================
# ITEM CLASS
#============================================================================

class Item(pcdm.Item):

    '''Class representing a paged repository item resource'''

    def __init__(self, id, item_map, root):
        super().__init__()
        self.id = id
        self.title = id
        self.path = os.path.join(root, item_map['metadata'])
        self.filepaths = [os.path.join(root, f) for f in item_map['files']]
        self.parts = item_map['parts'].items()
        self.sequence_attr = ('Page', 'id')
        self.root = root

    def read_data(self):
        self.title = next(self.graph().objects(predicate=dcterms.title))
        for path in self.filepaths:
            self.add_file(File.from_localpath(path))
        for (id, data) in self.parts:
            self.add_component(Page(id, data['files'], self))

    def graph(self):
        graph = super(Item, self).graph()
        if os.path.isfile(self.path):
            metadata = Graph().parse(self.path, format='turtle')
            for (s,p,o) in metadata:
                graph.add((self.uri, p, o))
        else:
            raise DataReadException(
                "File {0} not found".format(self.id + '.ttl')
                )
        return graph


#============================================================================
# PAGE (COMPONENT) CLASS
#============================================================================

class Page(pcdm.Component):

    '''Class representing one page of an item-level resource'''

    def __init__(self, id, files, item):
        super().__init__()
        self.id = str(id)
        self.title = "{0}, Page {1}".format(item.title, self.id)
        self.ordered = True
        for f in files:
            filepath = os.path.join(item.root, f)
            self.add_file(File.from_localpath(filepath))

    def graph(self):
        graph = super(Page, self).graph()
        graph.add((self.uri, dcterms.title, Literal(self.title)))
        graph.add((self.uri, rdf.type, fabio.Page))
        graph.add((self.uri, fabio.hasSequenceIdentifier, Literal(self.id)))
        return graph


#============================================================================
# FILE CLASS
#============================================================================

class File(pcdm.File):

    '''Class representing file associated with an item or page resource'''

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
