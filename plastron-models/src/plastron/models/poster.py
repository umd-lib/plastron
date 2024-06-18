from rdflib import URIRef

from plastron.models.pcdm import PCDMObject
from plastron.namespaces import bibo, dcterms, dc, edm, geo, ore, schema, umd, umdtype
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import ObjectProperty, DataProperty, Property
from plastron.validation.rules import is_edtf_formatted, is_handle, is_from_vocabulary


@rdf_type(bibo.Image, umd.Poster)
class Poster(PCDMObject):
    title = DataProperty(dcterms.title, required=True)
    alternative = DataProperty(dcterms.alternative)
    publisher = DataProperty(dc.publisher)
    part_of = DataProperty(dcterms.isPartOf, required=True)
    # use the full URI since "dc.format" returns the format method of Namespace
    format = DataProperty(URIRef('http://purl.org/dc/elements/1.1/format'), required=True)
    # this is a Property since the extant data in fcrepo contains both URIs and literals,
    # so neither ObjectProperty nor DataProperty would map all values correctly
    type = Property(edm.hasType, required=True)
    date = DataProperty(dc.date, validate=is_edtf_formatted)
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
    terms_of_use = ObjectProperty(
        dcterms.license,
        validate=is_from_vocabulary('http://vocab.lib.umd.edu/termsOfUse#')
    )
    copyright_notice = DataProperty(schema.copyrightNotice)
    identifier = DataProperty(dcterms.identifier, required=True)
    handle = DataProperty(dcterms.identifier, validate=is_handle, datatype=umdtype.handle)
    place = ObjectProperty(dcterms.spatial)
    presentation_set = ObjectProperty(
        ore.isAggregatedBy,
        repeatable=True,
        validate=is_from_vocabulary('http://vocab.lib.umd.edu/set#'),
    )

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
