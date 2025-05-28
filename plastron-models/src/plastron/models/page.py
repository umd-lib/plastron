from plastron.models import ContentModeledResource
from plastron.models.pcdm import PCDMObject, PCDMFile
from plastron.namespaces import fabio, pcdm, dcterms
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty


class File(ContentModeledResource, PCDMFile):
    model_name = 'File'
    is_top_level = False

    file_of = ObjectProperty(pcdm.fileOf)


@rdf_type(fabio.Page)
class Page(ContentModeledResource, PCDMObject):
    model_name = 'Page'
    is_top_level = False

    member_of = ObjectProperty(pcdm.memberOf)
    has_file = ObjectProperty(pcdm.hasFile, repeatable=True, cls=File)
    title = DataProperty(dcterms.title)
    number = DataProperty(fabio.hasSequenceIdentifier)

    def __str__(self):
        return str(self.title or self.uri)
