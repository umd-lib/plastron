from plastron import pcdm, rdf
from plastron.authority import LabeledThing
from plastron.namespaces import dc, dcterms, edm
from plastron.pcdm import Page
from plastron.validation import is_edtf_formatted, is_valid_iso639_code
from rdflib import Namespace


umdtype = Namespace('http://vocab.lib.umd.edu/datatype#')
umdform = Namespace('http://vocab.lib.umd.edu/form#')


@rdf.object_property('object_type', dcterms.type)
@rdf.data_property('identifier', dcterms.identifier)
@rdf.object_property('rights', dcterms.rights)
@rdf.data_property('title', dcterms.title)
@rdf.object_property('format', edm.hasType)
@rdf.object_property('archival_collection', dcterms.isPartOf)
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
@rdf.data_property('bibliographic_citation', dcterms.bibliographicCitation)
@rdf.data_property('accession_number', dcterms.identifier, datatype=umdtype.accessionNumber)
class Item(pcdm.Object):
    HEADER_MAP = {
        'object_type': 'Object Type',
        'identifier': 'Identifier',
        'rights': 'Rights Statement',
        'title': 'Title',
        'format': 'Format',
        'archival_collection': 'Archival Collection',
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
        'rights_holder.label': 'Rights Holder',
        'bibliographic_citation': 'Collection Information',
        'accession_number': 'Accession Number'
    }
    VALIDATION_RULESET = {
        'object_type': {
            'required': True,
            'exactly': 1,
            'from_vocabulary': 'http://purl.org/dc/dcmitype/',
        },
        'identifier': {
            'required': True
        },
        'rights': {
            'required': True,
            'exactly': 1,
            'from_vocabulary': 'http://vocab.lib.umd.edu/rightsStatement#'
        },
        'title': {
            'required': True,
            'exactly': 1
        },
        'format': {
            'from_vocabulary': 'http://vocab.lib.umd.edu/form#'
        },
        'archival_collection': {
            'max_values': 1,
            'from_vocabulary': 'http://vocab.lib.umd.edu/collection#'
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
        'rights_holder': {},
        'bibliographic_citation': {
            'max_values': 1
        },
        'accession_number': {
            'max_values': 1
        }
    }

    def get_new_member(self, rootname, number):
        if str(self.object_type) == str(umdform.pool_reports):
            if rootname.startswith('body-'):
                return Page(title=f'Body', number=number)
            else:
                return Page(title=f'Attachment {number - 1}', number=number)
        else:
            return Page(title=f'Page {number}', number=number)
