import os

from rdflib import Graph

from plastron.namespaces import get_manager
from plastron.rdfmapping.resources import RDFResourceBase

nsm = get_manager()


class TurtleSerializer:
    def __init__(self, directory, **_kwargs):
        self.directory_name = directory
        self.content_type = 'text/turtle'
        self.file_extension = '.ttl'
        self.graph = Graph(namespace_manager=nsm)

    def __enter__(self):
        return self

    def write(self, resource: RDFResourceBase, **_kwargs):
        self.graph += resource.graph

    def finish(self):
        with open(os.path.join(self.directory_name, 'metadata.ttl'), mode='wb') as export_file:
            self.graph.serialize(destination=export_file, format='turtle')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()
