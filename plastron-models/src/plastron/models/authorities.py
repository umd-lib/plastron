from rdflib import Graph

from plastron.namespaces import rdfs, owl, dcterms, rdf
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.vocabularies import get_vocabulary_graph


class LabeledThing(RDFResource):
    label = DataProperty(rdfs.label, required=True)
    same_as = ObjectProperty(owl.sameAs)


class Agent(LabeledThing):
    pass


class Subject(LabeledThing):
    pass


class Place(LabeledThing):
    pass


class VocabularyTerm(RDFResource):
    label = DataProperty(rdfs.label, required=True)
    description = DataProperty(dcterms.description)
    value = DataProperty(rdf.value)
    comment = DataProperty(rdfs.comment)
    same_as = ObjectProperty(owl.sameAs, repeatable=True)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # we just want the part with the term as the subject
        term_graph = Graph()
        for triple in get_vocabulary_graph(self.uri).triples((self.uri, None, None)):
            term_graph.add(triple)
        self._graph = term_graph
