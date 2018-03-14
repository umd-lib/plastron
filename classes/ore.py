from rdflib import Literal
from classes import ldp
from namespaces import dcterms, iana, ore, rdf

# alias the RDFlib Namespace
ns = ore

class Proxy(ldp.Resource):

    def __init__(self, position, proxy_for, proxy_in):
        super(Proxy, self).__init__()
        self.title = 'Proxy for {0} in {1}'.format(position, proxy_in.title)
        self.prev = None
        self.next = None
        self.proxy_for = proxy_for
        self.proxy_in = proxy_in

    def graph(self):
        graph = super(Proxy, self).graph()
        graph.add((self.uri, rdf.type, ore.Proxy))
        graph.add((self.uri, dcterms.title, Literal(self.title)))
        graph.add((self.uri, ore.proxyFor, self.proxy_for.uri))
        graph.add((self.uri, ore.proxyIn, self.proxy_in.uri))
        if self.prev is not None:
            graph.add((self.uri, iana.prev, self.prev.uri))
        if self.next is not None:
            graph.add((self.uri, iana.next, self.next.uri))
        return graph

    # create proxy object by PUTting object graph
    def create_object(self, repository, **kwargs):
        uri='/'.join([p.strip('/') for p in (self.proxy_for.uri, self.proxy_in.uuid)])
        super(Proxy, self).create_object(repository, uri=uri, **kwargs)
