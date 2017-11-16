'''On LDP, see http://www.w3.org/TR/2015/REC-ldp-20150226'''

import logging
from uuid import uuid4
from rdflib import Graph, URIRef
from datetime import datetime as dt
import namespaces

class Resource(object):
    '''Class representing a Linked Data Platform Resource (LDPR)
    A HTTP resource whose state is represented in any way that conforms to the
    simple lifecycle patterns and conventions in section 4. Linked Data Platform
    Resources.'''
    def __init__(self, uri=''):
        self.uri = URIRef(uri)
        self.linked_objects = []
        self.fragments = []
        self.annotations = None
        self.extra = Graph()
        self.created = False
        self.updated = False
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )

    def graph(self):
        graph = Graph()
        graph.namespace_manager = namespaces.get_manager(graph)

        for (rel, obj) in self.linked_objects:
            graph.add((self.uri, rel, obj.uri))

        for obj in self.fragments:
            graph = graph + obj.graph()

        graph = graph + self.extra

        return graph

    # create repository object by POST or PUT
    def create_object(self, repository, uri=None):
        if self.created:
            return False
        elif self.exists_in_repo(repository):
            self.created = True
            return False

        self.logger.info("Creating {0}...".format(self.title))
        if uri is not None:
            response = repository.put(uri)
        else:
            response = repository.post(repository.uri())

        if response.status_code == 201:
            self.created = True
            self.logger.info("Created {0}".format(self.title))
            self.uri = URIRef(
                repository._remove_transaction_uri(response.text)
                )
            self.uuid = str(self.uri).rsplit('/', 1)[-1]
            self.logger.info(
                'URI: {0} / UUID: {1}'.format(self.uri, self.uuid)
                )
            self.create_fragments()
            return True
        else:
            self.logger.error("Failed to create {0}".format(self.title))
            raise RESTAPIException(response)

    def create_fragments(self):
        for obj in self.fragments:
            obj.uuid = uuid4()
            obj.uri = URIRef('{0}#{1}'.format(self.uri, obj.uuid))
            obj.created = True

    # update existing repo object with SPARQL update
    def update_object(self, repository, patch_uri=None):
        graph = self.graph()
        if not patch_uri:
            patch_uri = self.uri
        prolog = ''
        #TODO: limit this to just the prefixes that are used in the graph
        for (prefix, uri) in graph.namespace_manager.namespaces():
            prolog += "PREFIX {0}: {1}\n".format(prefix, uri.n3())

        triples = []
        for (s, p, o) in graph:
            subject = s.n3(graph.namespace_manager)
            if '#' in subject:
                subject = '<' + subject[subject.index('#'):]
            else:
                subject = '<>'
            triples.append("{0} {1} {2}.".format(
                subject,
                graph.namespace_manager.normalizeUri(p),
                o.n3(graph.namespace_manager)
                ))

        query = prolog + "INSERT DATA {{{0}}}".format("\n".join(triples))
        data = query.encode('utf-8')
        headers = {'Content-Type': 'application/sparql-update'}
        self.logger.info("Updating {0}".format(self.title))
        response = repository.patch(str(patch_uri), data=data, headers=headers)
        if response.status_code == 204:
            self.logger.info("Updated {0}".format(self.title))
            self.updated = True
            return response
        else:
            self.logger.error("Failed to update {0}".format(self.title))
            self.logger.error(query)
            raise RESTAPIException(response)

    # recursively create an object and components and that don't yet exist
    def recursive_create(self, repository):
        if self.create_object(repository):
            self.creation_timestamp = dt.now()
        else:
            self.logger.debug('Object "{0}" exists. Skipping.'.format(self.title))

        for (rel, obj) in self.linked_objects:
            if obj.created or obj.exists_in_repo(repository):
                obj.created = True
                self.logger.debug('Object "{0}" exists. Skipping.'.format(self.title))
            else:
                obj.recursive_create(repository)

    # recursively update an object and all its components and files
    def recursive_update(self, repository):
        if not self.updated:
            self.update_object(repository)
            for (rel, obj) in self.linked_objects:
                obj.recursive_update(repository)

    # check for the existence of a local object in the repository
    def exists_in_repo(self, repository):
        if str(self.uri).startswith(repository.endpoint):
            response = repository.head(str(self.uri))
            if response.status_code == 200:
                return True
            else:
                return False
        else:
            return False

    # add arbitrary additional triples provided in a file
    def add_extra_properties(self, triples_file, rdf_format):
        self.extra.parse(
            source=triples_file, format=rdf_format, publicID=self.uri
            )

    # show the object's graph, serialized as turtle
    def print_graph(self):
        print(self.graph().serialize(format="turtle").decode())

    # called after creation of object in repo
    def post_creation_hook(self):
        pass

class RdfSource(Resource):
    '''Class representing a Linked Data Platform RDF Source (LDP-RS)
    An LDPR whose state is fully represented in RDF, corresponding to an RDF
    graph. See also the term RDF Source from [rdf11-concepts].'''
    def __init__(self):
        pass

class NonRdfSource(Resource):
    '''Class representing a Linked Data Platform Non-RDF Source (LDP-NR)
    An LDPR whose state is not represented in RDF. For example, these can be
    binary or text documents that do not have useful RDF representations.'''
    def __init__(self):
        pass

class Container(RdfSource):
    '''Class representing a Linked Data Platform Container (LDPC)
    A LDP-RS representing a collection of linked documents (RDF Document
    [rdf11-concepts] or information resources [WEBARCH]) that responds to client
    requests for creation, modification, and/or enumeration of its linked
    members and documents, and that conforms to the simple lifecycle patterns
    and conventions in section 5. Linked Data Platform Containers.'''
    def __init__(self):
        pass

class BasicContainer(Container):
    '''Class representing a Linked Data Platform Basic Container (LDP-BC)
    An LDPC that defines a simple link to its contained documents (information
    resources) [WEBARCH].'''
    def __init__(self):
        pass

class DirectContainer(Container):
    '''Class representing a Linked Data Platform Direct Container (LDP-DC)
    An LDPC that adds the concept of membership, allowing the flexibility of
    choosing what form its membership triples take, and allows members to be any
    resources [WEBARCH], not only documents.'''
    def __init__(self):
        pass

class IndirectContainer(Container):
    '''Class representing a Linked Data Platform Indirect Container (LDP-IC)
    An LDPC similar to a LDP-DC that is also capable of having members whose
    URIs are based on the content of its contained documents rather than the
    URIs assigned to those documents.'''
    def __init__(self):
        pass
