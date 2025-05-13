from plastron.handles import HandleBearingResource
from plastron.models import ContentModeledResource
from plastron.models.authorities import Agent, Subject, Place, DCMI_TYPES, UMD_RIGHTS_STATEMENTS, UMD_FORMATS, \
    UMD_ARCHIVAL_COLLECTIONS, UMD_PRESENTATION_SETS, UMD_TERMS_OF_USE_STATEMENTS
from plastron.models.fedora import FedoraResource
from plastron.models.page import File, Page
from plastron.models.pcdm import PCDMObject
from plastron.namespaces import dc, dcterms, edm, pcdm, ore, schema, umdtype, umd
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.rules import is_edtf_formatted, is_valid_iso639_code
from plastron.validation.vocabularies import ControlledVocabularyProperty


@rdf_type(pcdm.Object)
class Stub(RDFResource):
    title = DataProperty(dcterms.title, required=True)
    identifier = DataProperty(dcterms.identifier, required=True)
    member_of = ObjectProperty(pcdm.memberOf)


@rdf_type(pcdm.Collection)
class AdminSet(ContentModeledResource, RDFResource):
    model_name = 'AdminSet'
    is_top_level = False

    title = DataProperty(dcterms.title, required=True)


@rdf_type(umd.Item)
class Item(ContentModeledResource, PCDMObject, HandleBearingResource, FedoraResource):
    model_name = 'Item'
    is_top_level = True

    member_of = ObjectProperty(pcdm.memberOf)
    has_file = ObjectProperty(pcdm.hasFile, repeatable=True, cls=File)
    has_member = ObjectProperty(pcdm.hasMember, repeatable=True, cls=Page)
    object_type = ControlledVocabularyProperty(dcterms.type, required=True, vocab=DCMI_TYPES)
    identifier = DataProperty(dcterms.identifier, required=True, repeatable=True)
    rights = ControlledVocabularyProperty(dcterms.rights, required=True, vocab=UMD_RIGHTS_STATEMENTS)
    title = DataProperty(dcterms.title, required=True)
    format = ControlledVocabularyProperty(edm.hasType, vocab=UMD_FORMATS)
    archival_collection = ControlledVocabularyProperty(dcterms.isPartOf, vocab=UMD_ARCHIVAL_COLLECTIONS)
    presentation_set = ControlledVocabularyProperty(ore.isAggregatedBy, repeatable=True, vocab=UMD_PRESENTATION_SETS)
    date = DataProperty(dc.date, validate=is_edtf_formatted)
    description = DataProperty(dcterms.description)
    alternate_title = DataProperty(dcterms.alternative, repeatable=True)
    creator = ObjectProperty(dcterms.creator, repeatable=True, embed=True, cls=Agent)
    contributor = ObjectProperty(dcterms.contributor, repeatable=True, embed=True, cls=Agent)
    publisher = ObjectProperty(dcterms.publisher, repeatable=True, embed=True, cls=Agent)
    audience = ObjectProperty(dcterms.audience, repeatable=True, embed=True, cls=Agent)
    location = ObjectProperty(dcterms.spatial, repeatable=True, embed=True, cls=Place)
    extent = DataProperty(dcterms.extent, repeatable=True)
    subject = ObjectProperty(dcterms.subject, repeatable=True, embed=True, cls=Subject)
    language = DataProperty(dc.language, repeatable=True, validate=is_valid_iso639_code)
    rights_holder = ObjectProperty(dcterms.rightsHolder, repeatable=True, embed=True, cls=Agent)
    terms_of_use = ControlledVocabularyProperty(dcterms.license, vocab=UMD_TERMS_OF_USE_STATEMENTS)
    copyright_notice = DataProperty(schema.copyrightNotice)
    bibliographic_citation = DataProperty(dcterms.bibliographicCitation)
    accession_number = DataProperty(dcterms.identifier, datatype=umdtype.accessionNumber)

    HEADER_MAP = {
        'object_type': 'Object Type',
        'identifier': 'Identifier',
        'rights': 'Rights Statement',
        'title': 'Title',
        'format': 'Format',
        'archival_collection': 'Archival Collection',
        'presentation_set': 'Presentation Set',
        'date': 'Date',
        'description': 'Description',
        'alternate_title': 'Alternate Title',
        'creator': {
            'label': 'Creator',
            'same_as': 'Creator URI',
        },
        'contributor': {
            'label': 'Contributor',
            'same_as': 'Contributor URI',
        },
        'publisher': {
            'label': 'Publisher',
            'same_as': 'Publisher URI',
        },
        'audience': {
            'label': 'Audience',
            'same_as': 'Audience URI',
        },
        'location': {
            'label': 'Location',
        },
        'extent': 'Extent',
        'subject': {
            'label': 'Subject',
        },
        'language': 'Language',
        'rights_holder': {
            'label': 'Rights Holder',
        },
        'terms_of_use': 'Terms of Use',
        'copyright_notice': 'Copyright Notice',
        'bibliographic_citation': 'Collection Information',
        'accession_number': 'Accession Number',
        'handle': 'Handle',
    }

    def __str__(self):
        return str(self.title)
