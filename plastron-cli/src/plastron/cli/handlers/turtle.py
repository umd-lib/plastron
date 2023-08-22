""" A handler for loading sequenced assets from binaries & turtle metadata. """

import logging
import os

import yaml
from rdflib import Graph

from plastron.repo import DataReadError
from plastron.cli import ConfigError
from plastron.namespaces import dcterms
from plastron.rdf import pcdm, rdf
from plastron.rdf.authority import create_authority
from plastron.rdf.pcdm import get_file_object, Page

logger = logging.getLogger(__name__)


# ============================================================================
# BATCH CLASS (FOR PAGED BINARIES PLUS RDF METADATA)
# ============================================================================

class Batch:
    def __init__(self, repo, config):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )

        self.collection = pcdm.Collection.from_repository(repo, config.collection_uri)

        # Create data structures to accumulate process results
        self.incomplete = []
        self.extra_files = []

        # Check for required metadata file
        if not os.path.isfile(config.batch_file):
            raise ConfigError('Specified metadata file could not be found')

        # check for an existing file index
        file_index = os.path.join(config.data_dir, 'file_index.yml')
        if os.path.isfile(file_index):
            self.logger.info('Found file index in {0}'.format(file_index))
            with open(file_index, 'r') as index:
                self.all_files = yaml.safe_load(index)
        else:
            # Generate index of all files in the data path
            # maps the basename to a full path
            self.logger.info(f'Walking the {config.data_dir} tree to create a file index')
            self.all_files = {}
            file_count = 0
            for root, dirs, files in os.walk(config.data_dir):
                for f in files:
                    file_count += 1
                    if f not in self.all_files:
                        self.all_files[f] = [os.path.join(root, f)]
                    else:
                        self.all_files[f].append(os.path.join(root, f))

            self.logger.info("Found {0} files with {1} unique filenames"
                             .format(file_count, len(self.all_files)))

            # save index to file
            with open(file_index, 'w') as index:
                yaml.dump(self.all_files, index, default_flow_style=False)

        with open(config.batch_file, 'r') as f:
            self.logger.info(
                'Parsing the master metadata graph in {0}'.format(config.batch_file))
            self.master_graph = Graph().parse(f, format="turtle")

        # get subject URIs that are http: or https: URIs
        self.subjects = sorted(set([uri for uri in self.master_graph.subjects() if
                                    str(uri).startswith('http:') or str(uri).startswith('https:')]))

        # get the master list of authority objects
        # keyed by the urn:uuid:... URI from the master graph
        authority_subjects = set([uri for uri in self.master_graph.subjects()
                                  if str(uri).startswith('urn:uuid:')])
        self.authorities = {str(s): create_authority(self.master_graph, s)
                            for s in authority_subjects}

        self.length = len(self.subjects)
        self.count = 0
        self.logger.info("Batch contains {0} items.".format(self.length))

    def __iter__(self):
        return self

    def __next__(self):
        if self.count < self.length:
            item = BatchItem(self, self.subjects[self.count])
            self.count += 1
            return item
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()


class BatchItem:
    def __init__(self, batch, subject):
        self.batch = batch
        self.subject = subject
        self.path = str(subject)

    def read_data(self):
        item_graph = Graph()
        add_props = []
        for (s, p, o) in self.batch.master_graph.triples((self.subject, None, None)):
            item_graph.add((s, p, o))
            if str(o) in self.batch.authorities:
                # create an RDFObjectProperty for the triples with an
                # authority object value
                add_props.append(rdf.object_property(str(p), p, embed=True, obj_class=rdf.Resource))

        # dynamically-generated class based on predicates that are present in
        # the source graph
        cls = type(str(self.subject), (pcdm.Object,), {})
        for add_property in add_props:
            add_property(cls)

        item = cls.from_graph(item_graph, subject=self.subject)

        # set the value of the newly mapped properties to the correct
        # authority objects
        for prop in item.object_properties():
            if prop.is_embedded:
                prop.values = [self.batch.authorities[str(v.uri)] for v in prop.values]

        item.member_of = self.batch.collection

        files = []
        parts = {}

        # Parse each filename in hasPart and allocate to correct location in item entry
        for (s, p, o) in [(s, p, o) for (s, p, o) in item.unmapped_triples if p == dcterms.hasPart]:
            filename = str(o)
            # ensure exactly one path that is mapped from the basename
            if filename not in self.batch.all_files:
                raise DataReadError('File {0} not found'.format(filename))
            elif len(self.batch.all_files[filename]) > 1:
                raise DataReadError('Filename {0} is not unique'.format(filename))

            file_path = self.batch.all_files[filename][0]

            normalized = filename.replace('_', '-')
            basename, ext = os.path.splitext(normalized)
            base_parts = basename.split('-')

            # handle files with no sequence id
            if len(base_parts) == 2:
                files.append(file_path)
            # handle files with a sequence id
            elif len(base_parts) == 3:
                page_no = str(int(base_parts[2]))
                if page_no not in parts:
                    parts[page_no] = [file_path]
                else:
                    parts[page_no].append(file_path)
            else:
                logger.warning(
                    'Filename {0} does not match a known pattern'.format(filename))

        # remove the dcterms:hasPart triples
        item.unmapped_triples = [(s, p, o) for (s, p, o) in item.unmapped_triples if p != dcterms.hasPart]

        for path in files:
            item.add_file(get_file_object(path))

        # renumber the parts from 1
        for (n, key) in enumerate(sorted(parts.keys()), 1):
            page = Page(number=str(n), title=f"{item.title}, Page {n}")
            for path in parts[key]:
                page.add_file(get_file_object(path))
            item.add_member(page)
            # add ordering proxy for the page to the item
            item.append_proxy(page, title=f'Proxy for page {n} in {item.title}')

        return item
