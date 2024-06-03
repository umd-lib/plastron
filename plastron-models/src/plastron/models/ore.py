from plastron.namespaces import ore, dcterms, iana
from plastron.rdfmapping.decorators import rdf_type
from plastron.rdfmapping.descriptors import DataProperty, ObjectProperty
from plastron.rdfmapping.resources import RDFResource, RDFResourceBase


@rdf_type(ore.Proxy)
class Proxy(RDFResource):
    title = DataProperty(dcterms.title)
    prev = ObjectProperty(iana.prev, cls='Proxy')
    next = ObjectProperty(iana.next, cls='Proxy')
    proxy_for = ObjectProperty(ore.proxyFor, cls=RDFResourceBase)
    proxy_in = ObjectProperty(ore.proxyIn, cls=RDFResourceBase)


class AggregationMixin(RDFResourceBase):
    first = ObjectProperty(iana.first, cls=Proxy)
    last = ObjectProperty(iana.last, cls=Proxy)
