from plastron.namespaces import fedora, xsd
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty
from plastron.rdfmapping.resources import RDFResource


class FedoraResource(RDFResource):
    created = DataProperty(fedora.created, datatype=xsd.dateTime)
    created_by = DataProperty(fedora.createdBy)
    last_modified = DataProperty(fedora.lastModified, datatype=xsd.dateTime)
    last_modified_by = DataProperty(fedora.lastModifiedBy)
    parent = ObjectProperty(fedora.hasParent)
