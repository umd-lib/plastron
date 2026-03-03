from urlobject import URLObject


class Endpoint:
    """Conceptual entry point for a Fedora repository."""

    def __init__(self, url: str, default_path: str = '/'):
        self.url = URLObject(url)
        """Endpoint URL"""

        self.relpath = default_path
        """Default container path"""

        if not self.relpath.startswith('/'):
            self.relpath = '/' + self.relpath

    def __contains__(self, item: str) -> bool:
        return self.contains(item)

    def contains(self, uri: str) -> bool:
        """
        Returns `True` if the given URI string is contained within this
        repository, `False` otherwise. You may also use the builtin operator
        `in` to do this same check::

        ```pycon
        >>> endpoint = Endpoint(url='http://localhost:8080/fcrepo/rest')

        >>> endpoint.contains('http://localhost:8080/fcrepo/rest/123')
        True

        >>> 'http://localhost:8080/fcrepo/rest/123' in endpoint
        True

        >>> endpoint.contains('http://example.com/123')
        False

        >>> 'http://example.com/123' in endpoint
        False
        ```

        """
        return uri.startswith(self.url)

    def repo_path(self, resource_uri: str) -> str | None:
        """
        Returns the repository path for the given resource URI, i.e. the
        path with the ``url`` removed. For example:

        ```pycon
        >>> endpoint = Endpoint(url='http://localhost:8080/fcrepo/rest')

        >>> endpoint.repo_path('http://localhost:8080/fcrepo/rest/obj/123')
        '/obj/123'
        ```
        """
        if resource_uri is None:
            return None
        else:
            return resource_uri.replace(self.url, '')

    @property
    def transaction_endpoint(self) -> str:
        """Send an HTTP POST request to this URL to create a new transaction."""
        return self.url + '/fcr:tx'
