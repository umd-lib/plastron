from edtf import parse_edtf
from iso639 import is_valid639_1, is_valid639_2
from pyparsing import ParseException

from plastron import pcdm, rdf
from plastron.authority import LabeledThing
from plastron.namespaces import dc, dcterms, edm


def is_edtf_formatted(value):
    try:
        parse_edtf(value)
        return True
    except ParseException:
        return False


def is_valid_iso639_code(value):
    return is_valid639_1(value) or is_valid639_2(value)


@rdf.object_property('object_type', dcterms.type)
@rdf.data_property('identifier', dcterms.identifier)
@rdf.object_property('rights', dcterms.rights)
@rdf.data_property('title', dcterms.title)
@rdf.object_property('format', edm.hasType)
@rdf.object_property('archival_collection', dcterms.isPartOf, embed=True, obj_class=LabeledThing)
@rdf.data_property('date', dc.date)
@rdf.data_property('description', dcterms.description)
@rdf.data_property('alternate_title', dcterms.alternative)
@rdf.object_property('creator', dcterms.creator, embed=True, obj_class=LabeledThing)
@rdf.object_property('contributor', dcterms.contributor, embed=True, obj_class=LabeledThing)
@rdf.object_property('publisher', dcterms.publisher, embed=True, obj_class=LabeledThing)
@rdf.object_property('location', dcterms.spatial, embed=True, obj_class=LabeledThing)
@rdf.data_property('extent', dcterms.extent)
@rdf.object_property('subject', dcterms.subject, embed=True, obj_class=LabeledThing)
@rdf.data_property('language', dc.language)
@rdf.object_property('rights_holder', dcterms.rightsHolder, embed=True, obj_class=LabeledThing)
class Item(pcdm.Object):
    HEADER_MAP = {
        'object_type': 'Object Type',
        'identifier': 'Identifier',
        'rights': 'Rights Statement',
        'title': 'Title',
        'format': 'Format',
        'archival_collection.label': 'Archival Collection',
        'date': 'Date',
        'description': 'Description',
        'alternate_title': 'Alternate Title',
        'creator.label': 'Creator',
        'creator.same_as': 'Creator URI',
        'contributor.label': 'Contributor',
        'contributor.same_as': 'Contributor URI',
        'publisher.label': 'Publisher',
        'publisher.same_as': 'Publisher URI',
        'location.label': 'Location',
        'extent': 'Extent',
        'subject.label': 'Subject',
        'language': 'Language',
        'rights_holder.label': 'Rights Holder'
    }
    VALIDATION_RULESET = {
        'object_type': {
            'exactly': 1
        },
        'identifier': {
            'min_values': 1
        },
        'rights': {
            'exactly': 1
        },
        'title': {
            'exactly': 1
        },
        'format': {},
        'archival_collection': {
            'max_values': 1
        },
        'date': {
            'max_values': 1,
            'function': is_edtf_formatted
        },
        'description': {
            'max_values': 1
        },
        'alternate_title': {},
        'creator': {},
        'contributor': {},
        'publisher': {},
        'location': {},
        'extent': {},
        'subject': {},
        'language': {
            'function': is_valid_iso639_code
        },
        'rights_holder': {}
    }
