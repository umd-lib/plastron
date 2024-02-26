import logging
from datetime import datetime
from uuid import uuid4

from rdflib import Graph, URIRef

from plastron.client import ResourceURI, Client, ClientError
from plastron.rdf import rdf


class Resource(rdf.Resource):
    """Class representing a Linked Data Platform Resource (LDPR)
    A HTTP resource whose state is represented in any way that conforms to the
    simple lifecycle patterns and conventions in section 4. Linked Data Platform
    Resources."""

    @classmethod
    def from_repository(cls, client: Client, uri, include_server_managed=True):
        _, graph = client.get_graph(uri, include_server_managed=include_server_managed)
        obj = cls.from_graph(graph, subject=uri)
        obj.uri = uri
        obj.path = obj.uri[len(client.endpoint.url):]

        # get the description URI
        obj.resource = ResourceURI(uri, client.get_description_uri(uri))

        # mark as created and updated so that the create_object and update_object
        # methods doesn't try to modify it
        obj.created = True
        obj.updated = True

        return obj

    def __init__(self, uri='', **kwargs):
        super().__init__(uri=uri, **kwargs)
        self.annotations = []
        self.extra = Graph()
        self.created = False
        self.updated = False
        self.path = None
        self.container_path = None
        self.resource = None
        self.uuid = None
        self.creation_timestamp = None
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )

    def __str__(self):
        if hasattr(self, 'title') and self.title is not None:
            return str(self.title)
        else:
            return repr(self)

    def load(self, repository: Client):
        return self.read(repository.get_graph(self.uri)[1])

    def create(self, client: Client, container_path=None, slug=None, headers=None, recursive=True, **kwargs):
        if not self.created and not self.exists_in_repo(client):

            self.logger.info(f"Creating {self}...")
            if headers is None:
                headers = {}
            if slug is not None:
                headers['Slug'] = slug
            try:
                self.resource = client.create(container_path=container_path, headers=headers, **kwargs)
                self.uri = URIRef(self.resource.uri)

                # if client.is_forwarded():
                #     # Reverse forwarded URL back to fcrepo URL
                #     parsed_resource_uri = urlsplit(self.resource.uri)
                #     parsed_path = parsed_resource_uri.path
                #     self.path = parsed_path[len(client.endpoint_base_path):]
                # else:
                self.path = self.resource.uri[len(client.endpoint.url):]

                self.created = True
                # TODO: get this from the response headers
                self.creation_timestamp = datetime.now()
                # TODO: get the fedora:parent
                self.container_path = container_path or client.endpoint.relpath
                self.logger.info(f"Created {self}")
                self.uuid = str(self.uri).rsplit('/', 1)[-1]
                self.logger.info(f'URI: {self.uri}')
                self.create_fragments()
            except ClientError as e:
                self.logger.error(f"Failed to create {self}: {e}")
                raise
        else:
            self.created = True
            self.logger.debug(f'Object "{self}" exists. Skipping.')

        if recursive:
            client.create_annotations(self)

    def create_fragments(self):
        for obj in self.embedded_objects():
            obj.uuid = uuid4()
            obj.uri = URIRef('{0}#{1}'.format(self.uri, obj.uuid))
            obj.created = True

    def patch(self, client, sparql_update):
        headers = {'Content-Type': 'application/sparql-update'}
        self.logger.info(f"Updating {self}")
        response = client.patch(self.resource.description_uri, data=sparql_update, headers=headers)
        if response.status_code == 204:
            self.logger.info(f"Updated {self}")
            self.updated = True
            return response
        else:
            self.logger.error(f"Failed to update {self}")
            self.logger.error(sparql_update)
            self.logger.error(response.text)
            raise ClientError(response)

    # update existing repo object with SPARQL update
    def update(self, client, recursive=True):
        if not self.updated:
            # Ensure item being updated has a ResourceURI
            if self.resource is None:
                self.resource = ResourceURI(self.uri, client.get_description_uri(self.uri))

            self.patch(client, client.build_sparql_update(insert_graph=self.graph()))
            if recursive:
                # recursively update an object and all its components and files
                for obj in self.linked_objects():
                    obj.update(client)
                # update any annotations
                for annotation in self.annotations:
                    annotation.update(client)

    # check for the existence of a local object in the repository
    def exists_in_repo(self, client: Client):
        if str(self.uri).startswith(client.endpoint.url):
            return client.exists(str(self.uri))
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

    def update_annotations(self, client: Client):
        for annotation in self.annotations:
            annotation.update(client)


class RdfSource(Resource):
    """Class representing a Linked Data Platform RDF Source (LDP-RS)
    An LDPR whose state is fully represented in RDF, corresponding to an RDF
    graph. See also the term RDF Source from [rdf11-concepts]."""
    pass


class NonRdfSource(Resource):
    """Class representing a Linked Data Platform Non-RDF Source (LDP-NR)
    An LDPR whose state is not represented in RDF. For example, these can be
    binary or text documents that do not have useful RDF representations."""
    @classmethod
    def from_source(cls, source=None, **kwargs):
        obj = cls(**kwargs)
        obj.source = source
        return obj


class Container(RdfSource):
    """Class representing a Linked Data Platform Container (LDPC)
    A LDP-RS representing a collection of linked documents (RDF Document
    [rdf11-concepts] or information resources [WEBARCH]) that responds to client
    requests for creation, modification, and/or enumeration of its linked
    members and documents, and that conforms to the simple lifecycle patterns
    and conventions in section 5. Linked Data Platform Containers."""
    pass


class BasicContainer(Container):
    """Class representing a Linked Data Platform Basic Container (LDP-BC)
    An LDPC that defines a simple link to its contained documents (information
    resources) [WEBARCH]."""
    pass


class DirectContainer(Container):
    """Class representing a Linked Data Platform Direct Container (LDP-DC)
    An LDPC that adds the concept of membership, allowing the flexibility of
    choosing what form its membership triples take, and allows members to be any
    resources [WEBARCH], not only documents."""
    pass


class IndirectContainer(Container):
    """Class representing a Linked Data Platform Indirect Container (LDP-IC)
    An LDPC similar to a LDP-DC that is also capable of having members whose
    URIs are based on the content of its contained documents rather than the
    URIs assigned to those documents."""
    pass
