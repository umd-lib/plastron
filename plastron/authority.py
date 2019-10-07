from plastron import ldp, rdf
from plastron.namespaces import edm, geo, rdfs, owl


def create_authority(graph, subject):
    if (subject, rdf.ns.type, edm.Place) in graph:
        return Place.from_graph(graph, subject)
    else:
        return LabeledThing.from_graph(graph, subject)


@rdf.data_property('label', rdfs.label)
@rdf.object_property('same_as', owl.sameAs)
class LabeledThing(ldp.Resource):
    pass


@rdf.data_property('lat', geo.lat)
@rdf.data_property('lon', geo.long)
class Place(LabeledThing):
    pass
