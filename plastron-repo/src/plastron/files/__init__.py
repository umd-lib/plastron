import hashlib
import io
import re
import urllib
import zipfile
from dataclasses import dataclass, field
from http import HTTPStatus
from mimetypes import guess_type
from os.path import basename, isfile
from typing import Mapping, Any, Protocol, Optional, ClassVar
from urllib.parse import urlsplit

from paramiko import SFTPClient, SSHClient, AutoAddPolicy, SSHException
from paramiko.config import SSH_PORT
from rdflib import URIRef
from requests import Response, Session

from plastron.namespaces import pcdmuse, fabio


def get_ssh_client(sftp_uri: str | urllib.parse.SplitResult, **kwargs) -> SSHClient:
    """Create, connect, and return an `SSHClient` object. The username and hostname (and,
    optionally, the port) to connect to are taken from the `sftp_uri`. Additional keyword
    arguments in `**kwargs` are passed directly to the `SSHClient.connect()` method.

    Raises a `RuntimeError` if there are problems establishing the connection."""
    if isinstance(sftp_uri, str):
        sftp_uri = urlsplit(sftp_uri)
    if not isinstance(sftp_uri, urllib.parse.SplitResult):
        raise TypeError('Expects a str or a urllib.parse.SplitResult')
    ssh_client = SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy)
    try:
        ssh_client.connect(
            hostname=sftp_uri.hostname,
            username=sftp_uri.username,
            port=sftp_uri.port or SSH_PORT,
            **kwargs
        )
        return ssh_client
    except SSHException as e:
        raise RuntimeError(str(e)) from e


class DoesHTTPRequest(Protocol):
    """[Structural subtype](https://docs.python.org/3/library/typing.html#typing.Protocol)
    for HTTP client-like objects with a `request()` method that takes (at minimum) a `method`
    and `uri` argument, and returns a `requests.Response` object."""
    def request(self, method: str, uri: str, **kwargs) -> Response:
        ...


class BinarySourceError(Exception):
    """General class for errors with binary sources."""
    pass


class BinarySourceNotFoundError(BinarySourceError):
    """Raised when a binary source cannot be found."""
    pass


class BinarySource:
    """
    Base class for reading binary content from arbitrary locations.
    """

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        """This should return a file-like object."""
        raise NotImplementedError()

    def close(self):
        """This should clean up any resources associated with
        the file-like object returned by `open()`"""
        raise NotImplementedError()

    def mimetype(self) -> str:
        """Returns the MIME type of this binary source. It is left up to
        the individual implementations of `BinarySource` to decide how to
        best determine this information."""
        raise NotImplementedError()

    def exists(self) -> bool:
        """Returns `True` if this source exists, otherwise returns `False`."""
        raise NotImplementedError()

    def digest(self) -> str:
        """Generates the SHA-1 checksum. Returns a hex-encoded SHA-1 digest,
        prepended with the string "sha1="."""
        sha1 = hashlib.sha1()
        with self as stream:
            for block in stream:
                sha1.update(block)
        return 'sha1=' + sha1.hexdigest()

    @property
    def rdf_types(self) -> set[URIRef]:
        """Return a set of additional RDF types that describe this source.

        * `pcdmuse:PreservationMasterFile` if it has the `image/tiff` MIME type
        """
        if self.mimetype() == 'image/tiff':
            return {pcdmuse.PreservationMasterFile}
        return set()


class StringSource(BinarySource):
    """
    Binary source from an in-memory string. If no `mimetype` is specified, attempts
    to guess based on the `filename`, but falls back to the generic type
    `application/octet-stream` if there is no `filename` or the call to `guess_type()`
    fails.
    """
    def __init__(self, content: str, filename: str = '<str>', mimetype: str = None):
        self._content = content
        self.filename = filename
        if mimetype is None:
            mimetype = guess_type(filename)[0] or 'application/octet-stream'
        self._mimetype: str = mimetype
        self._buffer = None

    def __str__(self):
        return self.filename

    def open(self) -> io.BytesIO:
        """Returns an `io.BytesIO` object wrapper around the `content` string."""
        if self._buffer is None or self._buffer.closed:
            self._buffer = io.BytesIO(self._content.encode())
        return self._buffer

    def close(self):
        """Close the `io.BytesIO` object returned by `open()`."""
        self._buffer.close()

    def mimetype(self) -> str:
        """Returns the MIME type set in the constructor."""
        return self._mimetype

    def exists(self) -> bool:
        """Always returns `True`."""
        return True


class LocalFileSource(BinarySource):
    """
    A file on the local file system. If no `mimetype` is specified, attempts
    to guess based on the `localpath`.
    """
    def __init__(self, localpath: str, mimetype: str = None, filename=None):
        if mimetype is None:
            mimetype = guess_type(localpath)[0]
        self._mimetype = mimetype
        self.localpath = localpath
        self.filename = filename if filename is not None else basename(localpath)
        self._file = None

    def __str__(self):
        return self.localpath

    def open(self):
        """Opens `localpath` with mode `rb` and returns the handle. If `localpath`
        is not found, raises `BinarySourceNotFoundError`."""
        try:
            self._file = open(self.localpath, 'rb')
            return self._file
        except FileNotFoundError as e:
            raise BinarySourceNotFoundError(str(e)) from e

    def close(self):
        """Closes the open file handle."""
        if self._file is not None:
            self._file.close()

    def mimetype(self) -> str:
        """Returns the MIME type set in the constructor."""
        return self._mimetype

    def exists(self) -> bool:
        """Returns true if `localpath` exists and is a file."""
        return isfile(self.localpath)


class HTTPFileSource(BinarySource):
    """A binary retrievable over HTTP at the given URI. Any additional keyword arguments
    are stored and added to all `requests.request()` calls."""
    def __init__(self, uri, **kwargs):
        self.uri = uri
        """URI of the remote resource."""
        self.kwargs = kwargs
        """Additional keyword arguments that are added to all `requests.request()` calls."""
        self.filename = basename(self.uri)
        """Filename-only portion of `uri`."""
        self._mimetype = None
        self._client = Session()

    def __str__(self):
        return str(self.uri)

    def request(self, method: str, stream: bool = False) -> Response:
        """Send an HTTP request with the given `method` to this source's `uri`, and
        return the response."""
        return self._client.request(method, self.uri, **self.kwargs, stream=stream)

    def mimetype(self) -> str:
        """Returns the `Content-Type` header for a `HEAD` request to `uri`."""
        if self._mimetype is None:
            response = self.request('HEAD')
            self._mimetype = response.headers['Content-Type']
        return self._mimetype

    def open(self, chunk_size: int = 512):
        """Returns an iterator over the source's data, with the given
        `chunk_size` (defaults to `512`).

        If the response to the request is `404 Not Found`, raises a
        `BinarySourceNotFoundError`. If the response status is any other
        error status (>= 400), raises a `BinarySourceError`."""
        response = self.request('GET', stream=True)
        if not response.ok:
            if response.status_code == HTTPStatus.NOT_FOUND:
                raise BinarySourceNotFoundError(f'{response.status_code} {response.reason}: {self.uri}')
            else:
                raise BinarySourceError(response)
        return response.iter_content(chunk_size)

    def close(self):
        """This method does nothing (there is no special cleanup for HTTP requests)."""
        pass

    def exists(self) -> bool:
        """Returns `True` if a `HEAD` request to `uri` is successful."""
        return self.request('HEAD').ok


class RepositoryFileSource(HTTPFileSource):
    """A binary stored in a repository."""
    def __init__(self, uri: str, client: DoesHTTPRequest, **kwargs):
        super().__init__(uri, **kwargs)
        self._client = client


class RemoteFileSource(BinarySource):
    """A binary retrievable over SFTP."""
    def __init__(self, location: str, mimetype: str = None, ssh_options: Mapping[str, Any] = None):
        """
        :param location: the SFTP URI to the binary source, e.g., `sftp://user@example.com/path/to/file`
        :param mimetype: MIME type of the file. If not given, will attempt to detect by calling
            the `file` utility over an SSH connection.
        :param ssh_options: additional options to pass as keyword arguments to `SSHClient.connect()`
        """
        self._ssh_client = None
        self._sftp_client = None
        self.location = location
        self.sftp_uri = urlsplit(location)
        self.filename = basename(self.sftp_uri.path)
        self._mimetype = mimetype
        self.ssh_options = ssh_options or {}
        self._file = None

    def __str__(self):
        return self.location

    def close(self):
        """
        Closes the remote file handle and the SFTP and SSH clients.
        """
        if self._file is not None:
            self._file.close()
            self._file = None
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None
        if self._ssh_client is not None:
            self._ssh_client.close()
            self._ssh_client = None

    def ssh(self) -> SSHClient:
        if self._ssh_client is None:
            self._ssh_client = get_ssh_client(self.sftp_uri, **self.ssh_options)
        return self._ssh_client

    def sftp(self) -> SFTPClient:
        if self._sftp_client is None:
            self._sftp_client = SFTPClient.from_transport(self.ssh().get_transport())
        return self._sftp_client

    def ssh_exec(self, cmd) -> str:
        """Execute `cmd` over SSH, and return the first line of the remote STDOUT. Trailing
        newline is removed."""
        (stdin, stdout, stderr) = self.ssh().exec_command(cmd)
        return stdout.readline().rstrip('\n')

    def open(self):
        """Open the file over SFTP and return it as an `SFTPFile` object."""
        try:
            self._file = self.sftp().open(self.sftp_uri.path, mode='rb')
            return self._file
        except IOError as e:
            raise BinarySourceNotFoundError(str(e)) from e

    def mimetype(self) -> str:
        """Return the MIME type, as determined by running the `file` utility over SSH."""
        if self._mimetype is None:
            self._mimetype = self.ssh_exec(f'file --mime-type -F "" "{self.sftp_uri.path}"').split()[1]
        return self._mimetype

    def digest(self) -> str:
        sha1sum = self.ssh_exec(f'sha1sum "{self.sftp_uri.path}"').split()[0]
        return 'sha1=' + sha1sum

    def exists(self) -> bool:
        (_, stdout, _) = self.ssh().exec_command(f'test -f "{self.sftp_uri.path}"')
        return stdout.channel.recv_exit_status() == 0


class ZipFileSource(BinarySource):
    """
    A binary contained in a ZIP file.
    """
    def __init__(self, zip_file, path, mimetype=None, ssh_options=None):
        """
        :param zip_file: ZIP file. This may be a zipfile.ZipFile object,
            a string filename, an SFTP URI, or a readable file-like object.
        :param path: Path to a single binary stored within the ZIP file.
        :param mimetype: MIME type of the single binary. If not given,
            will attempt to guess based on the path given.
        :param ssh_options: additional options to pass as keyword arguments to SSHClient.connect
            (used when the zip_file is an SFTP URI)
        """
        self.ssh_options = ssh_options or {}
        self.zip_filename = None
        self.source = None

        self.path = path
        self.filename = basename(path)
        if mimetype is None:
            mimetype = guess_type(path)[0]
        self._mimetype = mimetype
        self.file = None

        if isinstance(zip_file, zipfile.ZipFile):
            self.zip_file = zip_file
            self.zip_filename = zip_file.filename
        else:
            # the zip_file arg is something other than a ZipFile
            # we delay creation of the ZipFile to avoid hitting
            # the filesystem, network, etc., until asked to read
            # from the zip file
            self.zip_file = None
            if isinstance(zip_file, str) and zip_file.startswith('sftp:'):
                self.source = RemoteFileSource(zip_file, self._mimetype, self.ssh_options)
            else:
                self.source = LocalFileSource(zip_file, self._mimetype, self.filename)

    def close(self):
        if self.file is not None:
            self.file.close()
        if self.source is not None:
            self.source.close()
        if self.zip_file is not None:
            self.zip_file = None

    def get_zip_file(self):
        if self.zip_file is not None:
            return self.zip_file
        if self.source is not None:
            try:
                self.zip_file = zipfile.ZipFile(self.source.open())
            except FileNotFoundError as e:
                raise BinarySourceNotFoundError(f'Zip file {self.source} not found') from e
            return self.zip_file

    def open(self):
        # open the desired file from the archive
        try:
            self.file = self.get_zip_file().open(self.path, 'r')
            return self.file
        except KeyError as e:
            raise BinarySourceNotFoundError(f"'{self.path}' not found in file '{self.source}'") from e

    def mimetype(self):
        return self._mimetype

    def exists(self):
        try:
            if self.source:
                with self.source:
                    self.get_zip_file().getinfo(self.path)
                    return True
            else:
                self.get_zip_file().getinfo(self.path)
                return True
        except KeyError:
            return False


@dataclass
class FileSpec:
    name: str
    usage: str = None
    source: BinarySource = None

    USAGE_TAGS: ClassVar[dict[str, set[URIRef]]] = {
        'preservation': {pcdmuse.PreservationMasterFile},
        'ocr': {pcdmuse.ExtractedText},
        'metadata': {fabio.MetadataFile},
    }

    @classmethod
    def parse(cls, data: str):
        name, usage = parse_usage_tag(data)
        return cls(name=name, usage=usage)

    def __str__(self):
        return self.name

    @property
    def spec(self) -> str:
        if self.usage:
            return f'<{self.usage}>{self.name}'
        else:
            return self.name

    @property
    def rdf_types(self) -> Optional[set[URIRef]]:
        if self.usage is not None:
            return self.USAGE_TAGS.get(self.usage.lower(), None)


@dataclass
class FileGroup:
    rootname: str
    label: str = None
    files: list[FileSpec] = field(default_factory=list)

    def __str__(self):
        extensions = list(map(lambda f: str(f).replace(self.rootname, ''), self.files))
        return f'{self.rootname}{{{",".join(extensions)}}}'

    @property
    def filenames(self) -> list[str]:
        return [file.name for file in self.files]

    def file(self, name: str) -> FileSpec:
        for file in self.files:
            if file.name == name:
                return file
        raise RuntimeError(f'{name} is not in file group {self}')


def parse_usage_tag(filename: str) -> tuple[str, Optional[str]]:
    if m := re.search(r'^<([^>]+)>(.*)', filename):
        return m[2], m[1]
    else:
        return filename, None
