from requests import Response
from urlobject import URLObject

from plastron.client.base import Client
from plastron.client.endpoint import Endpoint
from plastron.client.utils import SessionHeaderAttribute


class ProxiedClient(Client):
    """HTTP client that behaves as if it were sending reversed proxied requests
    to the repository. Takes both the standard `endpoint` parameter and an
    `origin_endpoint` for the actual URL to make requests to.

    Adds `X-Forwarded-Host` and `X-Forwarded-Proto` headers to requests, using
    the appropriate values from `endpoint`."""

    forwarded_host = SessionHeaderAttribute('X-Forwarded-Host')
    """`X-Forwarded-Host` header value, taken from the `endpoint` host (or host and port,
    if port is non-standard)."""
    forwarded_protocol = SessionHeaderAttribute('X-Forwarded-Proto')
    """`X-Forwarded-Proto` header value, taken from the `endpoint` URL scheme."""

    def __init__(self, endpoint: Endpoint, origin_endpoint: Endpoint, **kwargs):
        super().__init__(endpoint, **kwargs)
        self.origin_endpoint = origin_endpoint
        """Actual request URL."""

        forwarded_url = self.endpoint.url
        if forwarded_url.port:
            # fcrepo expects hostname and port in the X-Forwarded-Host header
            self.forwarded_host = f'{forwarded_url.hostname}:{forwarded_url.port}'
        else:
            self.forwarded_host = forwarded_url.hostname
        self.forwarded_protocol = forwarded_url.scheme

    def request(self, method: str, url: str, **kwargs) -> Response:
        """Swaps in the `origin_endpoint` for the external `endpoint` of the requested
        `url`, then sends the request using `Client.request`."""
        repo_path = self.endpoint.repo_path(url)
        proxied_url = self.origin_endpoint.url + repo_path
        return super().request(method, proxied_url, **kwargs)
