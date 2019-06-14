from rdflib import Literal
from plastron import ldp, rdf
from plastron.namespaces import dcterms, iana, ore

# alias the RDFlib Namespace
ns = ore

@rdf.object_property('prev', iana.prev)
@rdf.object_property('next', iana.next)
@rdf.object_property('proxy_for', ore.proxyFor)
@rdf.object_property('proxy_in', ore.proxyIn)
@rdf.data_property('title', dcterms.title)
@rdf.rdf_class(ore.Proxy)
class Proxy(ldp.Resource):
    # create proxy object by PUTting object graph
    def create_object(self, repository, **kwargs):
        uri='/'.join([p.strip('/') for p in (self.proxy_for.uri, self.proxy_in.uuid)])
        super().create_object(repository, uri=uri, **kwargs)
