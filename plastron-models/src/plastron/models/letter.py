from plastron.namespaces import bibo, dc, dcmitype, dcterms, edm, geo, rel, skos, ore, owl, umd, umdtype, schema
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.rules import is_edtf_formatted, is_handle
from plastron.validation.vocabularies import Vocabulary


class AuthorityRecord(RDFResource):
    same_as = ObjectProperty(owl.sameAs, repeatable=True)


@rdf_type(edm.Agent)
class Agent(AuthorityRecord):
    pass


@rdf_type(skos.Concept)
class Concept(AuthorityRecord):
    pass


@rdf_type(edm.Place)
class Place(AuthorityRecord):
    lat = DataProperty(geo.lat)
    long = DataProperty(geo.long)


@rdf_type(dcmitype.Collection)
class Collection(AuthorityRecord):
    pass


@rdf_type(bibo.Letter, umd.Letter)
class Letter(RDFResource):
    title = DataProperty(dcterms.title, required=True)
    author = ObjectProperty(rel.aut, repeatable=True, embed=True, cls=Agent)
    recipient = ObjectProperty(bibo.recipient, repeatable=True, embed=True, cls=Agent)
    part_of = ObjectProperty(dcterms.isPartOf, required=True, embed=True, cls=Collection)
    place = ObjectProperty(dcterms.spatial, repeatable=True, embed=True, cls=Place)
    subject = ObjectProperty(dcterms.subject, repeatable=True, embed=True, cls=Concept)
    rights = ObjectProperty(dcterms.rights, required=True)
    copyright_notice = DataProperty(schema.copyrightNotice)
    identifier = DataProperty(dcterms.identifier, required=True)
    type = DataProperty(edm.hasType, required=True)
    date = DataProperty(dc.date, validate=is_edtf_formatted)
    language = DataProperty(dc.language, required=True)
    description = DataProperty(dcterms.description, required=True)
    bibliographic_citation = DataProperty(dcterms.bibliographicCitation, required=True)
    extent = DataProperty(dcterms.extent, required=True)
    rights_holder = DataProperty(dcterms.rightsHolder, required=True)
    terms_of_use = ObjectProperty(
        dcterms.license,
        values_from=Vocabulary('http://vocab.lib.umd.edu/termsOfUse#'),
    )
    handle = DataProperty(dcterms.identifier, datatype=umdtype.handle, validate=is_handle)
    presentation_set = ObjectProperty(
        ore.isAggregatedBy,
        repeatable=True,
        values_from=Vocabulary('http://vocab.lib.umd.edu/set#'),
    )

    HEADER_MAP = {
        'title': 'Title',
        'rights_holder': 'Rights Holder',
        'extent': 'Extent',
        'bibliographic_citation': 'Bibliographic Citation',
        'description': 'Description',
        'language': 'Language',
        'date': 'Date',
        'type': 'Resource Type',
        'rights': 'Rights',
        'terms_of_use': 'Terms of Use',
        'copyright_notice': 'Copyright Notice',
        'subject': {
            'label': 'Subject',
        },
        'place': {
            'label': 'Location',
            'long': 'Longitude',
            'lat': 'Latitude',
        },
        'part_of': {
            'label': 'Archival Collection',
            'same_as': 'Handle/Link',
        },
        'identifier': 'Identifier',
        'recipient': {
            'label': 'Recipient',
        },
        'author': {
            'label': 'Author',
        },
        'handle': 'Handle',
        'presentation_set': 'Presentation Set',
    }
