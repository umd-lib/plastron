from plastron.client import Client
from plastron.namespaces import dcterms, iana, ore
from plastron.rdf import ldp, rdf

# alias the rdflib Namespace
ns = ore


@rdf.object_property('proxy_for', ore.proxyFor)
@rdf.object_property('proxy_in', ore.proxyIn)
@rdf.data_property('title', dcterms.title)
@rdf.rdf_class(ore.Proxy)
class Proxy(ldp.Resource):
    pass


# add these after the class definition, so Proxy can refer to itself
Proxy.add_object_property('prev', iana.prev, obj_class=Proxy)
Proxy.add_object_property('next', iana.next, obj_class=Proxy)


@rdf.object_property('first', iana.first, obj_class=Proxy)
@rdf.object_property('last', iana.last, obj_class=Proxy)
class Aggregation(ldp.Resource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.first = []
        self.last = []

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

    def load_proxies(self, client: Client):
        """
        Returns an AggregationIterator over the resources in this aggregation,
        fully filled in with data from the given repository.
        """
        return AggregationIterator(self, client)

    def __iter__(self):
        """
        Returns an AggregationIterator over the resources in this aggregation,
        without any further calls to a repository.
        """
        return AggregationIterator(self)

    def proxies(self):
        return [proxy for proxy in self]

    def create(self, client: Client, container_path=None, slug=None, headers=None, recursive=True, **kwargs):
        super().create(
            client=client,
            container_path=container_path,
            slug=slug,
            headers=headers,
            recursive=recursive,
            **kwargs
        )
        if recursive:
            client.create_proxies(self)


class AggregationIterator:
    """
    Iterator over the resources in an aggregation. If a repository is
    specified, then before each resource is returned, its metadata is
    retrieved from that repository.
    """
    def __init__(self, aggregation: Aggregation, client: Client = None):
        self.aggregation = aggregation
        self.repository = client
        if len(self.aggregation.first) == 0:
            # no members of the aggregation
            self.next_proxy = None
        else:
            self.next_proxy = self.aggregation.first.values[0]

    def __iter__(self):
        return self

    def __next__(self):
        if self.next_proxy is None:
            raise StopIteration()
        current_proxy = self.next_proxy
        if self.repository is not None:
            current_proxy.load(self.repository)
        try:
            self.next_proxy = current_proxy.next.values[0]
        except IndexError:
            # we have reached the last item of the aggregation
            # set next to None so the iterator will stop on the next iteration
            self.next_proxy = None
        return current_proxy
