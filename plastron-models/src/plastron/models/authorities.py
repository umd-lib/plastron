from plastron.namespaces import rdfs, owl
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.vocabularies import Vocabulary

DCMI_TYPES = Vocabulary('http://purl.org/dc/dcmitype/')
UMD_RIGHTS_STATEMENTS = Vocabulary('http://vocab.lib.umd.edu/rightsStatement#')
UMD_FORMATS = Vocabulary('http://vocab.lib.umd.edu/form#')
UMD_ARCHIVAL_COLLECTIONS = Vocabulary('http://vocab.lib.umd.edu/collection#')
UMD_PRESENTATION_SETS = Vocabulary('http://vocab.lib.umd.edu/set#')
UMD_TERMS_OF_USE_STATEMENTS = Vocabulary('http://vocab.lib.umd.edu/termsOfUse#')


class LabeledThing(RDFResource):
    label = DataProperty(rdfs.label, required=True)
    same_as = ObjectProperty(owl.sameAs)


class Agent(LabeledThing):
    pass


class Subject(LabeledThing):
    pass


class Place(LabeledThing):
    pass
