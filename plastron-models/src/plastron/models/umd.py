from plastron.handles import HandleBearingResource
from plastron.models.pcdm import PCDMObject
from plastron.namespaces import dc, dcterms, edm, rdfs, owl, fabio, pcdm, ore, schema, umdtype, umd
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.rules import is_edtf_formatted, is_valid_iso639_code, is_from_vocabulary


@rdf_type(pcdm.Object)
class Stub(RDFResource):
    title = DataProperty(dcterms.title, required=True)
    identifier = DataProperty(dcterms.identifier, required=True)
    member_of = ObjectProperty(pcdm.memberOf)


class LabeledThing(RDFResource):
    label = DataProperty(rdfs.label, required=True)
    same_as = ObjectProperty(owl.sameAs)


@rdf_type(umd.Item)
class Item(PCDMObject, HandleBearingResource):
    object_type = ObjectProperty(
        dcterms.type,
        required=True,
        validate=is_from_vocabulary('http://purl.org/dc/dcmitype/'),
    )
    identifier = DataProperty(dcterms.identifier, required=True, repeatable=True)
    rights = ObjectProperty(
        dcterms.rights,
        required=True,
        validate=is_from_vocabulary('http://vocab.lib.umd.edu/rightsStatement#'),
    )
    title = DataProperty(dcterms.title, required=True)
    format = ObjectProperty(edm.hasType, validate=is_from_vocabulary('http://vocab.lib.umd.edu/form#'))
    archival_collection = ObjectProperty(
        dcterms.isPartOf,
        validate=is_from_vocabulary('http://vocab.lib.umd.edu/collection#'),
    )
    presentation_set = ObjectProperty(
        ore.isAggregatedBy,
        repeatable=True,
        validate=is_from_vocabulary('http://vocab.lib.umd.edu/set#'),
    )
    date = DataProperty(dc.date, validate=is_edtf_formatted)
    description = DataProperty(dcterms.description)
    alternate_title = DataProperty(dcterms.alternative, repeatable=True)
    creator = ObjectProperty(dcterms.creator, repeatable=True, embed=True, cls=LabeledThing)
    contributor = ObjectProperty(dcterms.contributor, repeatable=True, embed=True, cls=LabeledThing)
    publisher = ObjectProperty(dcterms.publisher, repeatable=True, embed=True, cls=LabeledThing)
    location = ObjectProperty(dcterms.spatial, repeatable=True, embed=True, cls=LabeledThing)
    extent = DataProperty(dcterms.extent, repeatable=True)
    subject = ObjectProperty(dcterms.subject, repeatable=True, embed=True, cls=LabeledThing)
    language = DataProperty(dc.language, repeatable=True, validate=is_valid_iso639_code)
    rights_holder = ObjectProperty(dcterms.rightsHolder, repeatable=True, embed=True, cls=LabeledThing)
    terms_of_use = ObjectProperty(
        dcterms.license,
        validate=is_from_vocabulary('http://vocab.lib.umd.edu/termsOfUse#')
    )
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


@rdf_type(fabio.Page)
class Page(PCDMObject):
    title = DataProperty(dcterms.title)
    number = DataProperty(fabio.hasSequenceIdentifier)

    def __str__(self):
        return str(self.title or self.uri)
