from rdflib import URIRef

from plastron.handles import HandleBearingResource
from plastron.models import ContentModeledResource
from plastron.models.authorities import UMD_PRESENTATION_SETS, UMD_TERMS_OF_USE_STATEMENTS
from plastron.models.fedora import FedoraResource
from plastron.models.pcdm import PCDMObject
from plastron.models.page import Page
from plastron.namespaces import bibo, dcterms, dc, edm, geo, ore, schema, umd, xsd, pcdm
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty, Property
from plastron.validation.rules import is_edtf_formatted
from plastron.validation.vocabularies import ControlledVocabularyProperty


@rdf_type(bibo.Image, umd.Poster)
class Poster(ContentModeledResource, PCDMObject, HandleBearingResource, FedoraResource):
    model_name = 'Poster'
    is_top_level = True

    member_of = ObjectProperty(pcdm.memberOf)
    has_member = ObjectProperty(pcdm.hasMember, repeatable=True, cls=Page)
    title = DataProperty(dcterms.title, required=True)
    alternative = DataProperty(dcterms.alternative)
    publisher = DataProperty(dc.publisher)
    part_of = DataProperty(dcterms.isPartOf, required=True)
    # use the full URI since "dc.format" returns the format method of Namespace
    format = DataProperty(URIRef('http://purl.org/dc/elements/1.1/format'), required=True)
    # this is a Property since the extant data in fcrepo contains both URIs and literals,
    # so neither ObjectProperty nor DataProperty would map all values correctly
    type = Property(edm.hasType, required=True)
    date = DataProperty(dc.date, datatype=xsd.date, validate=is_edtf_formatted)
    language = DataProperty(dc.language, required=True)
    description = DataProperty(dcterms.description)
    extent = DataProperty(dcterms.extent)
    issue = DataProperty(bibo.issue)
    locator = DataProperty(bibo.locator, required=True)
    location = DataProperty(dc.coverage)
    longitude = DataProperty(geo.long)
    latitude = DataProperty(geo.lat)
    subject = DataProperty(dc.subject, repeatable=True)
    rights = ObjectProperty(dcterms.rights, required=True)
    terms_of_use = ControlledVocabularyProperty(dcterms.license, vocab=UMD_TERMS_OF_USE_STATEMENTS)
    copyright_notice = DataProperty(schema.copyrightNotice)
    identifier = DataProperty(dcterms.identifier, required=True)
    place = ObjectProperty(dcterms.spatial)
    presentation_set = ControlledVocabularyProperty(ore.isAggregatedBy, repeatable=True, vocab=UMD_PRESENTATION_SETS)

    HEADER_MAP = {
        'title': 'Title',
        'alternative': 'Alternate Title',
        'publisher': 'Publisher',
        'part_of': 'Collection',
        'format': 'Format',
        'type': 'Resource Type',
        'date': 'Date',
        'language': 'Language',
        'description': 'Description',
        'extent': 'Extent',
        'issue': 'Issue',
        'locator': 'Identifier/Call Number',
        'location': 'Location',
        'longitude': 'Longitude',
        'latitude': 'Latitude',
        'subject': 'Subject',
        'rights': 'Rights',
        'terms_of_use': 'Terms of Use',
        'copyright_notice': 'Copyright Notice',
        'identifier': 'Identifier',
        'handle': 'Handle',
        'presentation_set': 'Presentation Set',
    }
