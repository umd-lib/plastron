from plastron.namespaces import fedora
from plastron.rdfmapping.descriptors import ObjectProperty
from plastron.rdfmapping.resources import RDFResource


class FedoraResource(RDFResource):
    parent = ObjectProperty(fedora.hasParent)
