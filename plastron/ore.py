from plastron import ldp, rdf
from plastron.namespaces import dcterms, iana, ore

# alias the rdflib Namespace
ns = ore


@rdf.object_property('prev', iana.prev)
@rdf.object_property('next', iana.next)
@rdf.object_property('proxy_for', ore.proxyFor)
@rdf.object_property('proxy_in', ore.proxyIn)
@rdf.data_property('title', dcterms.title)
@rdf.rdf_class(ore.Proxy)
class Proxy(ldp.Resource):
    pass


@rdf.object_property('first', iana.first)
@rdf.object_property('last', iana.last)
class Aggregation(ldp.Resource):
    def append(self, proxy):
        if len(self.first) == 0:
            self.first = proxy
        else:
            last = self.last.values[0]
            # update the current last item to point to the new item
            last.next = proxy
            proxy.prev = last
        # new item is always the last in the list
        self.last = proxy
        return proxy

    def append_proxy(self, proxy_for, title=None):
        if title is None:
            title = f'Proxy for {proxy_for} in {self}'
        return self.append(Proxy(
            title=title,
            proxy_for=proxy_for,
            proxy_in=self
        ))

    def __iter__(self):
        if len(self.first) == 0:
            # no members of the aggregation
            self.next = None
        else:
            self.next = self.first.values[0]
        return self

    def __next__(self):
        if self.next is None:
            raise StopIteration()
        current = self.next
        try:
            self.next = current.next.values[0]
        except IndexError:
            # we have reached the last item of the aggregation
            # set next to None so the iterator will stop on the next iteration
            self.next = None
        return current

    def create_proxies(self, repository):
        for proxy in self:
            proxy.create_object(repository)
