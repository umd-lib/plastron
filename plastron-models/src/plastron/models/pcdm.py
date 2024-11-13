from plastron.models.ore import AggregationMixin
from plastron.namespaces import pcdm, dcterms, ebucore, premis, xsd
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.resources import RDFResource


@rdf_type(pcdm.Object)
class PCDMObject(RDFResource, AggregationMixin):
    title = DataProperty(dcterms.title)
    has_member = ObjectProperty(pcdm.hasMember, repeatable=True, cls='PCDMObject')
    member_of = ObjectProperty(pcdm.memberOf, repeatable=True)
    has_file = ObjectProperty(pcdm.hasFile, repeatable=True, cls='PCDMFile')


@rdf_type(pcdm.File)
class PCDMFile(RDFResource):
    title = DataProperty(dcterms.title)
    file_of = ObjectProperty(pcdm.fileOf, repeatable=True)
    mime_type = DataProperty(ebucore.hasMimeType)
    filename = DataProperty(ebucore.filename)
    size = DataProperty(premis.hasSize, datatype=xsd.long)
    checksum = ObjectProperty(premis.hasMessageDigest)

    def __str__(self):
        return str(self.title or self.uri)


class PCDMImageFile(PCDMFile):
    height = DataProperty(ebucore.height)
    width = DataProperty(ebucore.width)
