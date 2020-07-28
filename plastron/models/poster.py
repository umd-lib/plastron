from plastron import pcdm, rdf
from plastron.namespaces import dcterms, dc, edm, bibo, geo
from plastron.validation import is_edtf_formatted


@rdf.object_property('place', dcterms.spatial)
@rdf.object_property('rights', dcterms.rights)
@rdf.data_property('identifier', dc.identifier)
# must use the subscripting syntax, since dc.format returns the format method of Namespace
@rdf.data_property('format', dc['format'])
@rdf.data_property('type', edm.hasType)
@rdf.data_property('subject', dc.subject)
@rdf.data_property('location', dc.coverage)
@rdf.data_property('date', dc.date)
@rdf.data_property('language', dc.language)
@rdf.data_property('description', dcterms.description)
@rdf.data_property('extent', dcterms.extent)
@rdf.data_property('issue', bibo.issue)
@rdf.data_property('locator', bibo.locator)
@rdf.data_property('latitude', geo.lat)
@rdf.data_property('longitude', geo.long)
@rdf.data_property('part_of', dcterms.isPartOf)
@rdf.data_property('publisher', dc.publisher)
@rdf.data_property('alternative', dcterms.alternative)
@rdf.rdf_class(bibo.Image)
class Poster(pcdm.Object):
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
        'rights': 'Rights'
    }
    VALIDATION_RULESET = {
        'title': {
            'required': True
        },
        'alternative': {
            'required': True
        },
        # "place" is not currently exported
        'place': {},
        'rights': {
            'required': True,
            'exactly': 1
        },
        # "identifier" is not currently exported
        'identifier': {},
        'format': {},
        'type': {
            'required': True
        },
        'subject': {
            'required': True
        },
        'location': {
            'required': True
        },
        'date': {
            'required': True,
            'exactly': 1,
            'function': is_edtf_formatted
        },
        'language': {
            'required': True,
            'exactly': 1
        },
        'description': {},
        'extent': {
            'required': True,
            'exactly': 1
        },
        'issue': {
            'required': True,
            'exactly': 1
        },
        'locator': {
            'required': True,
            'exactly': 1
        },
        'latitude': {
            'required': True,
            'exactly': 1
        },
        'longitude': {
            'required': True,
            'exactly': 1
        },
        'part_of': {
            'required': True
        },
        'publisher': {
            'required': True
        }
    }
