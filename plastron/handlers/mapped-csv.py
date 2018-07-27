import csv
import logging
import os
import sys
import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.util import from_n3
from plastron import pcdm, ldp, namespaces
from plastron.util import LocalFile, RemoteFile
from plastron.exceptions import ConfigException, DataReadException
from plastron.namespaces import dcmitype, dcterms, pcdmuse, rdf
from collections import OrderedDict

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

        # load the metadata map and metadata file
        try:
            with open(self.metadata_map, 'r') as f:
                self.logger.info(f'Parsing the metadata map in {self.metadata_map}')
                self.mapping = yaml.safe_load(f)
            with open(self.metadata_file, 'r') as f:
                self.logger.info(f'Reading metadata file {self.metadata_file}')
                self.rows = [r for r in csv.DictReader(f)]
        except FileNotFoundError as e:
            raise ConfigException(e)

        key_column = get_flagged_column(self.mapping, 'key')
        if key_column is not None:
            self.length = len(set([line[key_column] for line in self.rows]))
        else:
            self.length = len(self.rows)

    def get_items(self, lines, mapping):
        key_column = get_flagged_column(mapping, 'key')
        filename_column = get_flagged_column(mapping, 'filename')

        if key_column is not None:
            # the lines are grouped into subjects by
            # the unique values of the key column
            key_conf = mapping[key_column]
            keys = OrderedDict.fromkeys([line[key_column] for line in lines])
            for key in keys:
                # add an item for each unique key
                sub_lines = [line for line in lines if line[key_column] == key]
                item = Item()
                item.path = key
                item.title = key
                item.ordered = False
                for column, conf in mapping.items():
                    set_value(item, column, conf, sub_lines[0])

                if 'members' in key_conf:
                    # this key_column is a subject with member items
                    for component in self.get_items(sub_lines, key_conf['members']):
                        item.add_component(component)
                elif 'files' in key_conf:
                    # this key_column is a subject with file items
                    for component in self.get_items(sub_lines, key_conf['files']):
                        item.add_file(component)

                yield item

        elif filename_column is not None:
            # this mapping is for file objects
            filename_conf = mapping[filename_column]
            for line in lines:
                filenames = line.get(filename_column, None)
                if filenames is not None:
                    if filename_conf.get('multivalued', False):
                        filenames = filenames.split(filename_conf['separator'])
                    else:
                        filenames = [ filenames ]

                    for f in filenames:
                        if 'host' in filename_conf:
                            source = RemoteFile(filename_conf['host'], f)
                        else:
                            # local file
                            localpath = os.path.join(self.data_path, f)
                            source = LocalFile(localpath)

                        file = File(source)
                        for column, conf in mapping.items():
                            set_value(file, column, conf, line)
                        yield file

        else:
            # each line is its own (implicit) subject
            # for an Item resource
            for line in lines:
                item = Item()
                for column, conf in mapping.items():
                    set_value(item, column, conf, line)
                yield item

    def __iter__(self):
        return self.get_items(self.rows, self.mapping)

#============================================================================
# ITEM CLASS
#============================================================================

class Item(pcdm.Item):
    '''Class representing a self-contained repository resource'''
    def __init__(self):
        super(Item, self).__init__()
        self.src_graph = Graph()

    def read_data(self):
        pass

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
    def __init__(self, *args, **kwargs):
        super(File, self).__init__(*args, **kwargs)
        self.src_graph = Graph()

    def graph(self):
        graph = super(File, self).graph()
        if self.src_graph is not None:
            for (s, p, o) in self.src_graph:
                graph.add((self.uri, p, o))
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


nsm = namespaces.get_manager()

def set_value(item, column, conf, line):
    if 'predicate' in conf:
        value = line.get(column, None)
        if value is None:
            # this is a "dummy" column that is not actually in the
            # source CSV file but should be generated, either from
            # a format-string pattern or a static value
            if 'pattern' in conf:
                value = conf['pattern'].format(**line)
            elif 'value' in conf:
                value = conf['value']
        pred_uri = from_n3(conf['predicate'], nsm=nsm)
        if conf.get('uriref', False):
            o = URIRef(from_n3(value, nsm=nsm))
        else:
            datatype = conf.get('datatype', None)
            if datatype is not None:
                datatype_uri = from_n3(datatype, nsm=nsm)
                o = Literal(value, datatype=datatype_uri)
            else:
                o = Literal(value)
        item.src_graph.add((item.uri, pred_uri, o))

def get_flagged_column(mapping, flag):
    cols = [ col for col in mapping if flag in mapping[col] and mapping[col][flag] ]
    if len(cols) > 1:
        raise ConfigException(f"Only one {flag} column per mapping level is allowed")
    elif len(cols) == 1:
        return cols[0]
    else:
        return None
