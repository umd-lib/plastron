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
                         'LOG_LOCATION',
                         'LOG_CONFIG',
                         'METADATA_FILE',
                         'DATA_PATH'
                         ]
        for key in required_keys:
            if not config.get(key):
                raise ConfigException(
                    'Missing required key {0} in batch config'.format(key)
                    )

        # Set configuration Properties
        self.local_path    = os.path.normpath(config.get('ROOT'))
        self.data_path     = os.path.join(self.local_path,
                                          config['DATA_PATH'])
        self.metadata_file = os.path.join(self.local_path,
                                          config.get('METADATA_FILE'))
        self.collection    = pcdm.Collection.from_repository(repo,
                                                     config.get('COLLECTION'))

        # Create data structures to accumulate process results
        self.incomplete  = []
        self.extra_files = []

        # Check for required metadata file
        if not os.path.isfile(self.metadata_file):
            raise ConfigException('Specified metadata file could not be found')

        # check for an existing file index
        file_index = os.path.join(self.local_path, 'file_index.yml')
        if os.path.isfile(file_index):
            self.logger.info('Found file index in {0}'.format(file_index))
            with open(file_index, 'r') as index:
                self.all_files = yaml.load(index)
        else:
            # Generate index of all files in the data path
            # maps the basename to a full path
            self.logger.info(
                    'Walking the {0} tree to create a file index'.format(self.data_path))
            self.all_files = {}
            for root, dirs, files in os.walk(self.data_path):
                for f in files:
                    if f in self.all_files:
                        raise DataReadException('Filename {0} is not unique'.format(f))
                    self.all_files[f] = os.path.join(root, f)

            self.logger.info("Found {0} files".format(len(self.all_files)))

            # save index to file
            with open(file_index, 'w') as index:
                yaml.dump(self.all_files, index, default_flow_style=False)

        with open(self.metadata_file, 'r') as f:
            self.logger.info(
                    'Parsing the master metadata graph in {0}'.format(self.metadata_file))
            self.master_graph = Graph().parse(f, format="turtle")

        self.subjects = sorted(set(self.master_graph.subjects()))
        self.length = len(self.subjects)
        self.count = 0
        self.logger.info("Batch contains {0} items.".format(self.length))

    def __iter__(self):
        return self

    def __next__(self):
        if self.count < self.length:
            subject = self.subjects[self.count]
            item_graph = Graph()
            for triple in self.master_graph.triples((subject, None, None)):
                item_graph.add(triple)
            item = Item.from_graph(item_graph, self.all_files)
            item.path = str(subject)
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

    @classmethod
    def from_graph(cls, graph, all_files):
        item = cls(title=next(graph.objects(predicate=dcterms.title)))
        item.src_graph = graph
        item.all_files = all_files
        return item

    def __init__(self, title=None, files=None, parts=None):
        super(Item, self).__init__()
        self.src_graph = None
        self.title = title
        self.filepaths = files
        self.parts = parts
        self.sequence_attr = ('Page', 'id')

    def read_data(self):
        files = []
        parts = {}

        if self.src_graph is not None:
            # Parse each filename in hasPart and allocate to correct location in item entry
            for part in self.src_graph.objects(predicate=dcterms.hasPart):
                filename = str(part)
                if not filename in self.all_files:
                    raise DataReadException('File {0} not found'.format(filename))

                normalized = filename.replace('_', '-')
                basename, ext = os.path.splitext(normalized)
                base_parts = basename.split('-')

                # handle files with no sequence id
                if len(base_parts) == 2:
                    files.append(self.all_files[filename])
                # handle files with a sequence id
                elif len(base_parts) == 3:
                    page_no = str(int(base_parts[2]))
                    if page_no not in parts:
                        parts[page_no] = Page(page_no, [self.all_files[filename]], self)
                    else:
                        parts[page_no].add_file(File.from_localpath(self.all_files[filename]))
                else:
                    item.logger.warning(
                            'Filename {0} does not match a known pattern'.format(filename))

                # remove the dcterms:hasPart triples
                self.src_graph.remove((None, dcterms.hasPart, part))

        for path in files:
            self.add_file(File.from_localpath(path))
        for id, page in parts.items():
            self.add_component(page)

    def graph(self):
        graph = super(Item, self).graph()
        if self.src_graph is not None:
            for (s, p, o) in self.src_graph:
                graph.add((self.uri, p, o))
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
            self.add_file(File.from_localpath(f))

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
