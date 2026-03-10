import logging
from typing import Optional, Type, Iterable, Iterator, TypeVar

from rdflib import URIRef
from urlobject import URLObject

from plastron.client.utils import random_slug
from plastron.models.ore import Proxy
from plastron.models.pcdm import PCDMObject
from plastron.rdfmapping.resources import RDFResourceBase
from plastron.repo import ContainerResource, Repository, RepositoryResource

logger = logging.getLogger(__name__)

T = TypeVar('T', bound='RepositoryResource')


class AggregationResource(ContainerResource):
    """An [ORE Aggregation](http://openarchives.org/ore/1.0/datamodel#Aggregation) resource"""
    def __init__(self, repo: Repository, path: str = None):
        super().__init__(repo, path)
        self.proxies_container: Optional[ContainerResource] = None

    def get_proxies(self) -> 'ProxyIterator[T]':
        """Iterates over the ordered proxies of this resource, and returns the proxies."""
        return ProxyIterator(self)

    def get_sequence(self, resource_type: Type[T] = None) -> 'ProxiedResourceIterator[T]':
        """Iterates over the ordered proxies of this resource, and returns
        the URLs of the proxied resources. If a `resource_type` is given,
        returns full objects of that type instead."""
        return ProxiedResourceIterator(self, resource_type)

    def create_proxy(self, proxy_for: RDFResourceBase, title: str) -> ContainerResource:
        """Create a proxy resource for the given target."""
        if self.proxies_container is None:
            logger.debug(f'Creating proxies container for {self.path}')
            self.proxies_container = self.create_child(resource_class=ContainerResource, slug='x')

        return self.proxies_container.create_child(
            resource_class=ContainerResource,
            description=Proxy(
                proxy_for=proxy_for,
                proxy_in=self.describe(PCDMObject),
                title=title,
            ),
            slug=random_slug(),
        )

    def create_sequence(self, descriptions: Iterable[PCDMObject]):
        proxy_sequence = []
        obj = self.describe(PCDMObject)
        for item in descriptions:
            proxy_sequence.append(self.create_proxy(
                proxy_for=item,
                title=item.title.value,
            ))

        if len(proxy_sequence) > 0:
            obj.first = URIRef(proxy_sequence[0].url)
            obj.last = URIRef(proxy_sequence[-1].url)

        for n, proxy_resource in enumerate(proxy_sequence):
            proxy = proxy_resource.describe(Proxy)
            if n > 0:
                # has a previous resource
                proxy.prev = URIRef(proxy_sequence[n - 1].url)
            if n < len(proxy_sequence) - 1:
                # has a next resource
                proxy.next = URIRef(proxy_sequence[n + 1].url)
            proxy_resource.update()
        self.update()


class ProxyIterator(Iterator[T]):
    """Iterator over the sequence of proxies of an `AggregationResource`. It begins
    by following the `iana:first` relation from the `resource` to the first proxy,
    and then follows the `iana:next` relations between the subsequent proxy resources.

    For each proxy in the sequence, it yields a `plastron.repo.RepositoryResource`
    object representing that proxy."""

    def __init__(self, resource: AggregationResource):
        self.resource: AggregationResource = resource
        """Aggregation resource"""
        self._repo: Repository = resource.repo
        self._next_proxy_uri = None

    def __iter__(self):
        self.resource.read()
        self._next_proxy_uri = self.resource.describe(PCDMObject).first.value
        return self

    def __next__(self):
        if self._next_proxy_uri is None:
            raise StopIteration
        current_proxy_resource = self._repo.get_resource(self._next_proxy_uri).read()
        self._next_proxy_uri = current_proxy_resource.describe(Proxy).next.value
        return current_proxy_resource


class ProxiedResourceIterator(ProxyIterator[T]):
    """Iterator over the sequence of proxied resources of an `AggregationResource`.
    It begins by following the `iana:first` relation from the `resource` to the
    first proxy, and then follows the `iana:next` relations between the subsequent
    proxy resources.

    For each proxy in the sequence, it yields the value of its `ore:proxyFor`
    relation as a `URLObject`. If a `resource_type` class is provided to the
    constructor, it instead returns an instance of that class. The provided class
    must be a subclass of `RepositoryResource`."""

    def __init__(self, resource: AggregationResource, resource_type: Type[T] = None):
        super().__init__(resource)
        self.resource_type = resource_type
        """Resource class to use to instantiate the proxied objects; if `None`, returns just the URL"""

    def __next__(self):
        proxy_resource = super().__next__()
        current_proxy = proxy_resource.describe(Proxy)
        url = URLObject(current_proxy.proxy_for.value)
        if self.resource_type is not None:
            return self._repo.get_resource(url, self.resource_type)
        else:
            return url
