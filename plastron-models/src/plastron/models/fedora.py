from plastron.namespaces import fedora, xsd
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty
from plastron.rdfmapping.resources import RDFResource


class FedoraResource(RDFResource):
    created = DataProperty(fedora.created, datatype=xsd.dateTime)
    last_modified = DataProperty(fedora.lastModified, datatype=xsd.dateTime)
    parent = ObjectProperty(fedora.hasParent)
