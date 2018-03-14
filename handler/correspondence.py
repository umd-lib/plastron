''' A handler for loading correspondence from binaries & turtle metadata. '''

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
                         'DATA_PATH',
                         'INCLUDE_PATTERN'
                         ] 
        for key in required_keys:
            if not config.get(key):
                raise ConfigException(
                    'Missing required key {0} in batch config'.format(key)
                    )
        self.letters         = {}
        self.extra_files   = []
        self.local_path    = os.path.normpath(config.get('ROOT'))
        self.index_file    = os.path.join(self.local_path,
                                          config.get('BATCH_INDEX'))
        self.pattern       = re.compile(config['INCLUDE_PATTERN'], re.VERBOSE)
        self.data_path     = os.path.join(self.local_path, config['DATA_PATH'])
        self.metadata_path = os.path.join(self.local_path, 
                                          config['METADATA_PATH'])
        self.metadata_file = os.path.join(self.local_path,
                                          config.get('METADATA_FILE'))
        self.collection    = pcdm.Collection.from_repository(repo,
                                                     config.get('COLLECTION'))

        # Check for required metadata file and path
        if not os.path.isdir(self.metadata_path):
            os.mkdir(self.metadata_path)
        if not os.path.isfile(self.metadata_file):
            raise ConfigException('Specified metadata file could not be found')

        # Generate item-level metadata graphs and store as files
        with open(self.metadata_file, 'r') as f:    
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

        # Generate the batch index by reading from file or walking directories
        if os.path.isfile(self.index_file):
            self.logger.info("Reading batch index from {0}".format(
                                                        self.index_file))
            with open(self.index_file, 'r') as infile:
                self.letters = yaml.load(infile)
        else:
            # Walk directory and construct items by file name
            for (dir, subdirs, files) in os.walk(self.data_path):
                for f in files:
                    fullpath = os.path.join(dir, f)
                    relpath  = os.path.relpath(fullpath, self.local_path)
                    basename = os.path.basename(relpath)
                    match    = re.match(self.pattern, basename)

                    if match:
                        groups = match.groupdict()
                        item_id = '-'.join([groups['proj'], groups['ser_no']])
                        # create resource entry if it doesn't exist
                        if not item_id in self.letters:
                            self.letters[item_id] = {'files': [],
                                                     'parts': {},
                                                     'metadata': ''
                                                     }
                        current_item = self.letters[item_id]
                        # add file directly to resource if no sequence number
                        if groups['seq_no'] is None:
                            current_item['files'].append(relpath)
                        else:
                            parts = current_item['parts']
                            part_id = str(int(groups['seq_no']))
                            # create part if it doesn't exist
                            if part_id not in parts:
                                parts[part_id] = {'files': [], 'parts': {}}
                            # append file path to existing part
                            parts[part_id]['files'].append(relpath)
                    else:
                        # add files not fitting the pattern to extra files list
                        self.extra_files.append(relpath)

            # Add existing metadata files to the batch index
            for id in self.letters:
                expected_meta = os.path.join(self.metadata_path, id) + '.ttl'
                if os.path.isfile(expected_meta):
                    rel_meta = os.path.relpath(expected_meta, self.local_path)
                    self.letters[id]['metadata'] = rel_meta

            # Serialize the index to a YAML file
            self.logger.info("Serializing index to {0}".format(
                                                        self.index_file))
            with open(self.index_file, 'w') as outfile:
                yaml.dump(self.letters, outfile, default_flow_style=False)

        # Create list of complete item keys and set up counters
        self.to_load = sorted(self.letters.keys())
        self.length = len(self.to_load)
        self.count = 0
        self.logger.info("Batch contains {0} items.".format(self.length))

    def __iter__(self):
        return self

    def __next__(self):
        if self.count < self.length:
            id = self.to_load[self.count]
            item_map = self.letters[id]
            letter = Letter(id, item_map, self.local_path)
            letter.add_collection(self.collection)
            self.count += 1
            return letter
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()


#============================================================================
# LETTER (ITEM) CLASS
#============================================================================

class Letter(pcdm.Item):

    '''Class representing a letter'''

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
        for (id, parts) in self.parts:
            self.add_component(Page(id, parts['files'], self))

    def graph(self):
        graph = super(Letter, self).graph()
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

    '''Class representing a page of a letter'''

    def __init__(self, id, files, item):
        super().__init__()
        self.id = id
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

    '''Class representing file associated with a letter or page resource'''

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
