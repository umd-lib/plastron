from plastron import pcdm, rdf
from plastron.namespaces import dcterms, dc, edm, bibo, geo


@rdf.object_property('place', dcterms.spatial)
@rdf.object_property('rights', dcterms.rights)
@rdf.data_property('identifier', dc.identifier)
@rdf.data_property('format', dc.format)
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
            'min_values': 1
        },
        'alternative': {
            'min_values': 1
        },
        # "place" is not currently exported
        'place': {},
        'rights': {
            'exactly': 1
        },
        # "identifier" is not currently exported
        'identifier': {},
        'format': {},
        'type': {
            'min_values': 1
        },
        'subject': {
            'exactly': 1
        },
        'location': {
            'min_values': 1
        },
        'date': {
            'exactly': 1
        },
        'language': {
            'exactly': 1
        },
        'description': {},
        'extent': {
            'exactly': 1
        },
        'issue': {
            'exactly': 1
        },
        'locator': {
            'exactly': 1
        },
        'latitude': {
            'exactly': 1
        },
        'longitude': {
            'exactly': 1
        },
        'part_of': {
            'min_values': 1
        },
        'publisher': {
            'min_values': 1
        }
    }
