from rdflib import Namespace

from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty
from plastron.rdfmapping.resources import RDFResource
from plastron.validation.rules import is_edtf_formatted, is_handle, is_from_vocabulary
from plastron.namespaces import bibo, dc, dcmitype, dcterms, edm, geo, ore, owl, rel, schema, skos, umd

umdtype = Namespace('http://vocab.lib.umd.edu/datatype#')


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
        validate=is_from_vocabulary('http://vocab.lib.umd.edu/termsOfUse#')
    )
    handle = DataProperty(dcterms.identifier, datatype=umdtype.handle, validate=is_handle)
    presentation_set = ObjectProperty(
        ore.isAggregatedBy,
        repeatable=True,
        validate=is_from_vocabulary('http://vocab.lib.umd.edu/set#'),
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


"""
@rdf.object_property('author', rel.aut, embed=True, obj_class=Author)
@rdf.object_property('recipient', bibo.recipient, embed=True, obj_class=Recipient)
@rdf.object_property('part_of', dcterms.isPartOf, embed=True, obj_class=Collection)
@rdf.object_property('place', dcterms.spatial, embed=True, obj_class=Place)
@rdf.object_property('subject', dcterms.subject, embed=True, obj_class=Subject)
@rdf.object_property('rights', dcterms.rights)
@rdf.data_property('identifier', dcterms.identifier)
@rdf.data_property('type', edm.hasType)
@rdf.data_property('date', dc.date)
@rdf.data_property('language', dc.language)
@rdf.data_property('description', dcterms.description)
@rdf.data_property('bibliographic_citation', dcterms.bibliographicCitation)
@rdf.data_property('extent', dcterms.extent)
@rdf.data_property('rights_holder', dcterms.rightsHolder)
@rdf.data_property('handle', dcterms.identifier, datatype=umdtype.handle)
@rdf.rdf_class(bibo.Letter)
class Letter(pcdm.Object):
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
        'subject.label': 'Subject',
        'place.label': 'Location',
        'place.lon': 'Longitude',
        'place.lat': 'Latitude',
        'part_of.label': 'Archival Collection',
        'part_of.same_as': 'Handle/Link',
        'identifier': 'Identifier',
        'recipient.label': 'Recipient',
        'author.label': 'Author',
        'handle': 'Handle'
    }
    VALIDATION_RULESET = {
        'author': {
        },
        'recipient': {
        },
        'part_of': {
            'required': True,
            'exactly': 1
        },
        'place': {
        },
        'subject': {
        },
        'rights': {
            'required': True,
            'exactly': 1
        },
        'identifier': {
            'required': True
        },
        'type': {
            'required': True,
            'exactly': 1
        },
        'date': {
            # Can't do "exactly 1", because that makes it required
            # 'exactly': 1,
            'function': is_edtf_formatted
        },
        'language': {
            'required': True,
            'exactly': 1
        },
        'description': {
            'required': True,
            'exactly': 1
        },
        'bibliographic_citation': {
            'required': True,
            'exactly': 1
        },
        'extent': {
            'required': True,
            'exactly': 1
        },
        'rights_holder': {
            'required': True,
            'exactly': 1
        },
        'title': {
            'required': True,
            'exactly': 1
        },
        'handle': {
            'required': False,
            # 'exactly': 1,
            'function': is_handle
        },
    }
"""
