import csv
import logging
import os
from collections import OrderedDict

import yaml
from rdflib import Literal, URIRef
from rdflib.util import from_n3

from plastron.cli import ConfigError
from plastron.files import LocalFileSource, RemoteFileSource
from plastron.rdf import pcdm, rdf
from plastron import namespaces

nsm = namespaces.get_manager()


class Batch:
    """Class representing the mapped and parsed CSV data"""

    def __init__(self, repo, config):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        # Set configuration properties
        self.collection = pcdm.Collection.from_repository(repo, config.collection_uri)

        missing_fields = []
        try:
            self.file_path = os.path.join(config.data_dir, config.handler_options['FILE_PATH'])
        except KeyError:
            missing_fields.append('FILE_PATH')
        try:
            self.metadata_map = os.path.join(config.data_dir, config.handler_options['METADATA_MAP'])
        except KeyError:
            missing_fields.append('METADATA_MAP')

        if missing_fields:
            field_names = ', '.join(missing_fields)
            raise ConfigError(f'Missing required HANDLER_OPTIONS in batch configuration: {field_names}')

        if 'RDF_TYPE' in config.handler_options:
            self.item_rdf_type = URIRef(from_n3(config.handler_options['RDF_TYPE'], nsm=nsm))
        else:
            self.item_rdf_type = None

        # load the metadata map and metadata file
        try:
            with open(self.metadata_map, 'r') as f:
                self.logger.info(f'Parsing the metadata map in {self.metadata_map}')
                self.mapping = yaml.safe_load(f)
            with open(config.batch_file, 'r') as f:
                self.logger.info(f'Reading metadata file {config.batch_file}')
                self.rows = [r for r in csv.DictReader(f)]
        except FileNotFoundError as e:
            raise ConfigError(e)

        key_column = get_flagged_column(self.mapping, 'key')
        if key_column is not None:
            self.length = len(set([line[key_column] for line in self.rows]))
        else:
            self.length = len(self.rows)

    def get_items(self, lines, mapping):
        cls = create_class_from_mapping(mapping, self.item_rdf_type)

        key_column = get_flagged_column(mapping, 'key')
        filename_column = get_flagged_column(mapping, 'filename')
        dirname_column = get_flagged_column(mapping, 'dirname')

        if key_column is not None:
            # the lines are grouped into subjects by
            # the unique values of the key column
            key_conf = mapping[key_column]
            keys = OrderedDict.fromkeys([line[key_column] for line in lines])
            for key in keys:
                # add an item for each unique key
                sub_lines = [line for line in lines if line[key_column] == key]
                attrs = {column: get_column_value(sub_lines[0], column, mapping) for column in mapping.keys()}
                item = cls(**attrs)
                item.path = key
                item.ordered = False
                item.sequence_attr = ('Page', 'number')

                # add any members or files
                if 'members' in key_conf:
                    # this key_column is a subject with member items
                    for component in self.get_items(sub_lines, key_conf['members']):
                        # there may also be files that should be directly
                        # associated with the item
                        if isinstance(component, pcdm.File):
                            item.add_file(component)
                        else:
                            item.add_member(component)
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
                        filenames = [filenames]

                    for filename in filenames:
                        if 'host' in filename_conf:
                            source = RemoteFileSource(filename_conf['host'], filename)
                        else:
                            # local file
                            localpath = os.path.join(self.file_path, filename)
                            source = LocalFileSource(localpath)

                        f = pcdm.get_file_object(filename, source)
                        for column, conf in mapping.items():
                            set_value(f, column, conf, line)
                        yield f

        elif dirname_column is not None:
            # this mapping describes a directory of files that should be
            # subdivided based on filename into member objects
            dirname_conf = mapping[dirname_column]
            for line in lines:
                dirname = line.get(dirname_column, None)
                if dirname is not None:
                    members = {}
                    for entry in os.scandir(os.path.join(self.file_path, dirname)):
                        base, ext = os.path.splitext(entry.name)
                        if base not in members:
                            members[base] = []
                        members[base].append(entry)

                    for key in members:
                        key_parts = key.split('-')
                        if len(key_parts) == 2:
                            # top-level
                            for entry in members[key]:
                                source = LocalFileSource(entry.path)
                                f = pcdm.get_file_object(entry.name, source)
                                yield f

                        elif len(key_parts) == 3:
                            # part
                            # TODO: this number only makes sense as part of the
                            # folder; should be the title of the proxy object
                            sequence_number = int(key_parts[2])
                            page = pcdm.Page(number=str(sequence_number), title=f'Page {sequence_number}')
                            for entry in members[key]:
                                source = LocalFileSource(entry.path)
                                f = pcdm.get_file_object(entry.name, source)
                                for column, conf in mapping.items():
                                    set_value(f, column, conf, line)
                                page.add_file(f)
                            yield page

        else:
            # each line is its own (implicit) subject
            # for an Item resource
            for line in lines:
                attrs = {column: get_column_value(line, column, mapping) for column in mapping.keys()}
                item = cls(**attrs)
                yield item

    def __iter__(self):
        for item in self.get_items(self.rows, self.mapping):
            item.member_of = self.collection
            yield BatchItem(item)


class BatchItem:
    def __init__(self, item):
        self.item = item
        self.path = item.path

    def read_data(self):
        return self.item


# dynamically-generated class based on column names and predicates that are
# present in the mapping
def create_class_from_mapping(mapping, rdf_type=None):
    cls = type('csv', (pcdm.Object,), {})
    for column, conf in mapping.items():
        if 'predicate' in conf:
            pred_uri = from_n3(conf['predicate'], nsm=nsm)
            if conf.get('uriref', False):
                add_property = rdf.object_property(column, pred_uri)
            else:
                if 'datatype' in conf:
                    datatype = from_n3(conf['datatype'], nsm=nsm)
                else:
                    datatype = None
                add_property = rdf.data_property(column, pred_uri, datatype=datatype)
            add_property(cls)

    if rdf_type is not None:
        add_type = rdf.rdf_class(rdf_type)
        add_type(cls)

    return cls


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
            try:
                o = URIRef(from_n3(value, nsm=nsm))
            except KeyError:
                # prefix not found, assume it is not a prefixed form
                o = URIRef(value)
        else:
            datatype = conf.get('datatype', None)
            if datatype is not None:
                datatype_uri = from_n3(datatype, nsm=nsm)
                o = Literal(value, datatype=datatype_uri)
            else:
                o = Literal(value)
        item.unmapped_triples.append((item.uri, pred_uri, o))


def get_flagged_column(mapping, flag):
    cols = [col for col in mapping if flag in mapping[col] and mapping[col][flag]]
    if len(cols) > 1:
        raise ConfigError(f"Only one {flag} column per mapping level is allowed")
    elif len(cols) == 1:
        return cols[0]
    else:
        return None


def get_column_value(row, column, mapping):
    conf = mapping[column]
    value = row.get(column, None)
    if value is None:
        # this is a "dummy" column that is not actually in the
        # source CSV file but should be generated, either from
        # a format-string pattern or a static value
        if 'pattern' in conf:
            value = conf['pattern'].format(**row)
        elif 'value' in conf:
            value = conf['value']
    if conf.get('uriref', False):
        try:
            return URIRef(from_n3(value, nsm=nsm))
        except KeyError:
            # prefix not found, assume it is not a prefixed form
            return URIRef(value)
    else:
        return value
