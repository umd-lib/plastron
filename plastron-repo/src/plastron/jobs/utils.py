import logging
import re
from collections import OrderedDict, defaultdict
from os.path import splitext, basename
from uuid import uuid4

from bs4 import BeautifulSoup
from rdflib import Literal, URIRef, Graph
from rdflib.util import from_n3

from plastron.core.exceptions import DataReadException
from plastron.namespaces import sc, get_manager
from plastron.rdf import rdf
from plastron.rdf.oa import TextualBody, FullTextAnnotation
from plastron.rdf.rdf import RDFDataProperty, Resource
from plastron.serializers import CSVSerializer

logger = logging.getLogger(__name__)
nsm = get_manager()


def get_property_type(model_class: rdf.Resource, attrs):
    if '.' in attrs:
        first, rest = attrs.split('.', 2)
        return get_property_type(model_class.name_to_prop[first].obj_class, rest)
    else:
        return model_class.name_to_prop[attrs]


def build_lookup_index(item: Resource, index_string: str):
    """
    Build a lookup dictionary for embedded object properties of an item.

    :param item:
    :param index_string:
    :return:
    """
    index = defaultdict(dict)
    if index_string is None:
        return index

    pattern = r'([\w]+)\[(\d+)\]'
    for entry in index_string.split(';'):
        key, uriref = entry.split('=')
        m = re.search(pattern, key)
        attr = m[1]
        i = int(m[2])
        prop = getattr(item, attr)
        try:
            index[attr][i] = prop[URIRef(item.uri + uriref)]
        except IndexError:
            # need to create an object with that URI
            obj = prop.obj_class(uri=URIRef(item.uri + uriref))
            # TODO: what if i > 0?
            prop.values.append(obj)
            index[attr][i] = obj
    return index


def build_fields(fieldnames, model_class):
    property_attrs = {header: attrs for attrs, header in model_class.HEADER_MAP.items()}
    fields = defaultdict(list)
    # group typed and language-tagged columns by their property attribute
    for header in fieldnames:
        if '[' in header:
            # this field has a language tag
            # header format is "Header Label [Language Label]"
            header_label, language_label = re.search(r'^([^[]+)\s+\[(.+)]$', header).groups()
            try:
                attrs = property_attrs[header_label]
            except KeyError as e:
                raise DataReadException(f'Unknown header "{header}" in import file.') from e
            # if the language label isn't a name in the LANGUAGE_CODES table,
            # assume that it is itself a language code
            lang_code = CSVSerializer.LANGUAGE_CODES.get(language_label, language_label)
            fields[attrs].append({
                'header': header,
                'lang_code': lang_code,
                'datatype': None
            })
        elif '{' in header:
            # this field has a datatype
            # header format is "Header Label {Datatype Label}
            header_label, datatype_label = re.search(r'^([^{]+)\s+{(.+)}$', header).groups()
            try:
                attrs = property_attrs[header_label]
            except KeyError as e:
                raise DataReadException(f'Unknown header "{header}" in import file.') from e
            # the datatype label should either be a key in the lookup table,
            # or an n3-abbreviated URI of a datatype
            try:
                datatype_uri = CSVSerializer.DATATYPE_URIS.get(datatype_label, from_n3(datatype_label, nsm=nsm))
                if not isinstance(datatype_uri, URIRef):
                    raise DataReadException(f'Unknown datatype "{datatype_label}" in "{header}" in import file.')
            except KeyError as e:
                raise DataReadException(f'Unknown datatype "{datatype_label}" in "{header}" in import file.') from e

            fields[attrs].append({
                'header': header,
                'lang_code': None,
                'datatype': datatype_uri
            })
        else:
            # no language tag or datatype
            # make sure we skip the system columns
            if header not in CSVSerializer.SYSTEM_HEADERS:
                if header not in property_attrs:
                    raise DataReadException(f'Unrecognized header "{header}" in import file.')
                # check for a default datatype defined in the model
                attrs = property_attrs[header]
                prop = model_class.name_to_prop.get(attrs)
                if prop is not None and issubclass(prop, RDFDataProperty):
                    datatype_uri = prop.datatype
                else:
                    datatype_uri = None
                fields[attrs].append({
                    'header': header,
                    'lang_code': None,
                    'datatype': datatype_uri
                })
    return fields


def build_file_groups(filenames_string):
    file_groups = OrderedDict()
    if filenames_string.strip() == '':
        return file_groups
    for filename in filenames_string.split(';'):
        root, ext = splitext(basename(filename))
        if root not in file_groups:
            file_groups[root] = []
        file_groups[root].append(filename)
    logger.debug(f'Found {len(file_groups.keys())} unique file basename(s)')
    return file_groups


def not_empty(value):
    return value is not None and value != ''


def split_escaped(string: str, separator: str = '|'):
    # uses a negative look-behind to only split on separator characters
    # that are NOT preceded by an escape character (the backslash)
    pattern = re.compile(r'(?<!\\)' + re.escape(separator))
    values = pattern.split(string)
    # remove the escape character
    return [re.sub(r'\\(.)', r'\1', v) for v in values]


def parse_value_string(value_string, column, prop_type):
    # filter out empty strings, so we don't get spurious empty values in the properties
    for value in filter(not_empty, split_escaped(value_string, separator='|')):
        if issubclass(prop_type, RDFDataProperty):
            # default to the property's defined datatype
            # if it was not specified in the column header
            yield Literal(value, lang=column['lang_code'], datatype=column.get('datatype', prop_type.datatype))
        else:
            yield URIRef(value)


def annotate_from_files(item, mime_types):
    for member in item.members:
        # extract text from HTML files
        for file in filter(lambda f: str(f.mimetype) in mime_types, member.files):
            if str(file.mimetype) == 'text/html':
                # get text from HTML
                with file.source as stream:
                    text = BeautifulSoup(b''.join(stream), features='lxml').get_text()
            else:
                logger.warning(f'Extracting text from {file.mimetype} is not supported')
                continue

            annotation = FullTextAnnotation(
                target=member,
                body=TextualBody(value=text, content_type='text/plain'),
                motivation=sc.painting,
                derived_from=file
            )
            # don't embed full resources
            annotation.props['target'].is_embedded = False

            member.annotations.append(annotation)


def create_repo_changeset(repo, metadata, row, validate_only=False):
    """
    Returns a RepoChangeset of the changes to make to the repository

    :param repo: the repository configuration
    :param metadata: A plastron.jobs.MetadataRows object representing the
                      CSV file for the import
    :param row: A single plastron.jobs.Row object representing the row
                 to import
    :param validate_only: If true, will not fetch existing object from the
                repository.
    :return: A RepoChangeSet encapsulating the changes to make to the
            repository.
    """
    if validate_only:
        # create an empty object to validate without fetching from the repo
        item = metadata.model_class(uri=row.uri)
    else:
        if row.uri is not None:
            # read the object from the repo
            item = metadata.model_class.from_repository(repo, row.uri, include_server_managed=False)
        else:
            # no URI in the CSV means we will create a new object
            logger.info(f'No URI found for {row.line_reference}; will create new resource')
            # create a new object (will create in the repo later)
            item = metadata.model_class()

    # track new embedded objects that are added to the graph
    # so we can ensure that they have at least one statement
    # where they appear as the subject
    new_objects = defaultdict(Graph)

    delete_graph = Graph()
    insert_graph = Graph()

    # build the lookup index to map hash URI objects
    # to their correct positional locations
    row_index = build_lookup_index(item, row.index_string)

    for attrs, columns in metadata.fields.items():
        prop_type = get_property_type(item.__class__, attrs)
        if '.' not in attrs:
            # simple, non-embedded values
            # attrs is the entire property name
            new_values = []
            for column in columns:
                header = column['header']
                new_values.extend(parse_value_string(row[header], column, prop_type))

            # construct a SPARQL update by diffing for deletions and insertions
            # update the property and get the sets of values deleted and inserted
            prop = getattr(item, attrs)
            deleted_values, inserted_values = prop.update(new_values)

            for deleted_value in deleted_values:
                delete_graph.add((item.uri, prop.uri, prop.get_term(deleted_value)))
            for inserted_value in inserted_values:
                insert_graph.add((item.uri, prop.uri, prop.get_term(inserted_value)))

        else:
            # complex, embedded values

            # if the first portion of the dotted attr notation is a key in the index,
            # then this column has a different subject than the main uri
            # correlate positions and urirefs
            # XXX: for now, assuming only 2 levels of chaining
            first_attr, next_attr = attrs.split('.', 2)
            new_values = defaultdict(list)
            for column in columns:
                header = column['header']
                for i, value_string in enumerate(row[header].split(';')):
                    new_values[i].extend(parse_value_string(value_string, column, prop_type))

            if first_attr in row_index:
                # existing embedded object
                for i, values in new_values.items():
                    # get the embedded object
                    obj = row_index[first_attr][i]
                    prop = getattr(obj, next_attr)
                    deleted_values, inserted_values = prop.update(values)

                    for deleted_value in deleted_values:
                        delete_graph.add((item.uri, prop.uri, prop.get_term(deleted_value)))
                    for inserted_value in inserted_values:
                        insert_graph.add((item.uri, prop.uri, prop.get_term(inserted_value)))
            else:
                # create new embedded objects (a.k.a hash resources) that are not in the index
                first_prop_type = item.name_to_prop[first_attr]
                for i, values in new_values.items():
                    # we can assume that for any properties with dotted notation,
                    # all attributes except for the last one are object properties
                    if first_prop_type.obj_class is not None:
                        # create a new object
                        # TODO: remove hardcoded UUID fragment minting
                        obj = first_prop_type.obj_class(uri=f'{item.uri}#{uuid4()}')
                        # add the new object to the index
                        row_index[first_attr][i] = obj
                        setattr(obj, next_attr, values)
                        next_attr_prop = obj.name_to_prop[next_attr]
                        for value in values:
                            new_objects[(first_attr, obj)].add((obj.uri, next_attr_prop.uri, value))

    # add new embedded objects to the insert graph
    for (attr, obj), graph in new_objects.items():
        # add that object to the main item
        getattr(item, attr).append(obj)
        # add to the insert graph
        insert_graph.add((item.uri, item.name_to_prop[attr].uri, obj.uri))
        insert_graph += graph

    # do a pass to remove statements that are both deleted and then re-inserted
    for statement in delete_graph:
        if statement in insert_graph:
            delete_graph.remove(statement)
            insert_graph.remove(statement)

    return RepoChangeset(item, insert_graph, delete_graph)


class RepoChangeset:
    """
    Data object encapsulating the set of changes that need to be made to
    the repository for a single import

    :param item: a repository model object (i.e. from plastron.models) from
                 the repository (or an empty object if validation only)
    :param insert_graph: an RDF Graph object to insert into the repository
    :param delete_graph: an RDF Graph object to delete from the repository
    """
    def __init__(self, item, insert_graph, delete_graph):
        self._item = item
        self._insert_graph = insert_graph
        self._delete_graph = delete_graph

    @property
    def item(self):
        return self._item

    @property
    def insert_graph(self):
        return self._insert_graph

    @property
    def delete_graph(self):
        return self._delete_graph

    @property
    def is_empty(self):
        return len(self.insert_graph) == 0 and len(self.delete_graph) == 0

    def __bool__(self):
        return not self.is_empty

    def build_sparql_update(self, repo):
        return repo.build_sparql_update(self.delete_graph, self.insert_graph)
