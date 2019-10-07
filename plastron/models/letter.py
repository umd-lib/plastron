from plastron import rdf, pcdm
from plastron.authority import LabeledThing
from plastron.namespaces import bibo, dc, dcmitype, dcterms, edm, geo, rel, skos


@rdf.rdf_class(edm.Agent)
class Author(LabeledThing):
    pass


@rdf.rdf_class(edm.Agent)
class Recipient(LabeledThing):
    pass


@rdf.rdf_class(skos.Concept)
class Subject(LabeledThing):
    pass


@rdf.rdf_class(edm.Place)
@rdf.data_property('lat', geo.lat)
@rdf.data_property('lon', geo.long)
class Place(LabeledThing):
    pass


@rdf.rdf_class(dcmitype.Collection)
class Collection(LabeledThing):
    pass


@rdf.object_property('author', rel.aut, embed=True, obj_class=Author)
@rdf.object_property('recipient', bibo.recipient, embed=True, obj_class=Recipient)
@rdf.object_property('part_of', dcterms.isPartOf, embed=True, obj_class=Collection)
@rdf.object_property('place', dcterms.spatial, embed=True, obj_class=Place)
@rdf.object_property('subject', dcterms.subject, embed=True, obj_class=Subject)
@rdf.object_property('rights', dcterms.rights)
@rdf.data_property('type', edm.hasType)
@rdf.data_property('date', dc.date)
@rdf.data_property('language', dc.language)
@rdf.data_property('description', dcterms.description)
@rdf.data_property('bibliographic_citation', dcterms.bibliographicCitation)
@rdf.data_property('extent', dcterms.extent)
@rdf.data_property('rights_holder', dcterms.rightsHolder)
@rdf.rdf_class(bibo.Letter)
class Letter(pcdm.Item):
    HEADER_MAP = {
        'title': 'Title',
        # 'related_of',
        # 'related',
        # 'file_of',
        # 'files',
        # 'collections',
        # 'components',
        # 'last',
        # 'first',
        'rights_holder': 'Rights Holder',
        'extent': 'Extent',
        'bibliographic_citation': 'Bibliographic Citation',
        'description': 'Description',
        'language': 'Language',
        'date': 'Date',
        'type': 'Resource Type',
        'rights': 'Rights',
        # 'subject.same_as',
        'subject.label': 'Subject',
        # 'place.same_as',
        'place.label': 'Location',
        'place.lon': 'Longitude',
        'place.lat': 'Latitude',
        # 'part_of.same_as',
        'part_of.label': 'Archival Collection',
        # 'recipient.same_as',
        'recipient.label': 'Recipient',
        # 'author.same_as',
        'author.label': 'Author'
    }
