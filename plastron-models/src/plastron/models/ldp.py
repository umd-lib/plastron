from plastron.namespaces import ldp
from plastron.rdfmapping.descriptors import ObjectProperty
from plastron.rdfmapping.resources import RDFResource


class LDPContainer(RDFResource):
    contains = ObjectProperty(ldp.contains, repeatable=True)
