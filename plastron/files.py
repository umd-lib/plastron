import hashlib
import io
import zipfile
import requests
from mimetypes import guess_type
from os.path import basename, isfile
from paramiko import SFTPClient
from plastron.exceptions import BinarySourceError, BinarySourceNotFoundError, RESTAPIException
from plastron.util import get_ssh_client
from rdflib import URIRef
from urllib.parse import urlsplit


class BinarySource:
    """
    Base class for reading binary content from arbitrary locations.
    """

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        # this should return a file-like object
        raise NotImplementedError()

    def close(self):
        # this should clean up any resources associated with
        # the file-like object returned by open()
        raise NotImplementedError()

    def mimetype(self):
        raise NotImplementedError()

    def exists(self):
        raise NotImplementedError()

    def digest(self):
        """
        Generates the SHA-1 checksum.

        :returns: Hex encoded SHA-1 digest, prepended with the string "sha1="
        """
        sha1 = hashlib.sha1()
        with self as stream:
            for block in stream:
                sha1.update(block)
        return 'sha1=' + sha1.hexdigest()


class StringSource(BinarySource):
    """
    Binary source from an in-memory string.
    """
    def __init__(self, content, filename='<str>', mimetype=None):
        self.content = content
        self.filename = filename
        if mimetype is None:
            mimetype = guess_type(filename)[0] or 'application/octet-stream'
        self._mimetype = mimetype
        self.buffer = None

    def __str__(self):
        return self.filename

    def open(self):
        if self.buffer is None or self.buffer.closed:
            self.buffer = io.StringIO(self.content)
        return self.buffer

    def close(self):
        self.buffer.close()

    def mimetype(self):
        return self._mimetype

    def exists(self):
        return True


class LocalFileSource(BinarySource):
    """
    A file on the local file system.
    """
    def __init__(self, localpath, mimetype=None, filename=None):
        if mimetype is None:
            mimetype = guess_type(localpath)[0]
        self._mimetype = mimetype
        self.localpath = localpath
        self.filename = filename if filename is not None else basename(localpath)
        self.file = None

    def __str__(self):
        return self.localpath

    def open(self):
        try:
            self.file = open(self.localpath, 'rb')
            return self.file
        except FileNotFoundError as e:
            raise BinarySourceNotFoundError(str(e)) from e

    def close(self):
        if self.file is not None:
            self.file.close()

    def mimetype(self):
        return self._mimetype

    def exists(self):
        return isfile(self.localpath)


class HTTPFileSource(BinarySource):
    """
    A binary retrievable over HTTP.
    """
    def __init__(self, uri, **kwargs):
        self.uri = uri
        self.kwargs = kwargs
        self.filename = basename(self.uri)
        self._mimetype = None

    def __str__(self):
        return str(self.uri)

    def request(self, method, stream=False):
        response = requests.request(method, self.uri, **self.kwargs, stream=stream)

        if response.status_code == 404:
            raise BinarySourceNotFoundError(f'{response.status_code} {response.reason}: {self.uri}')
        if response.status_code != 200:
            raise BinarySourceError(response)

        return response

    def mimetype(self):
        if self._mimetype is None:
            response = self.request('HEAD')
            self._mimetype = response.headers['Content-Type']
        return self._mimetype

    def open(self, chunk_size=512):
        return self.request('GET', stream=True).iter_content(chunk_size)

    def close(self):
        # no special cleanup for HTTP requests
        pass

    def exists(self):
        return requests.request('HEAD', self.uri).status_code == 200


class RepositoryFileSource(BinarySource):
    """
    A binary stored in a repository.
    """
    def __init__(self, repo, file_uri):
        file_uri = URIRef(file_uri)
        response = repo.head(file_uri)

        if response.status_code == 404:
            raise BinarySourceNotFoundError(f'{response.status_code} {response.reason}: {file_uri}')
        if response.status_code != 200:
            raise RESTAPIException(response)

        self._mimetype = response.headers['Content-Type']

        self.repo = repo
        self.file_uri = file_uri

    def __str__(self):
        return str(self.file_uri)

    def mimetype(self):
        return self._mimetype

    def open(self, chunk_size=512):
        return self.repo.get(self.file_uri, stream=True).iter_content(chunk_size)

    def close(self):
        # no special cleanup for HTTP requests
        pass

    def exists(self):
        return self.repo.exists(self.file_uri)


class RemoteFileSource(BinarySource):
    """
    A binary retrievable by SFTP.
    """
    def __init__(self, location, mimetype=None, ssh_options=None):
        """
        :param location: the SFTP URI to the binary source, "sftp://user@example.com/path/to/file"
        :param mimetype: MIME type of the file. If not given, will attempt to detect by calling
            the "file" utility over an SSH connection.
        :param ssh_options: additional options to pass as keyword arguments to SSHClient.connect
        """
        self.ssh_client = None
        self.sftp_client = None
        self.location = location
        self.sftp_uri = urlsplit(location)
        self.filename = basename(self.sftp_uri.path)
        self._mimetype = mimetype
        self.ssh_options = ssh_options or {}
        self.file = None

    def __str__(self):
        return self.location

    def close(self):
        """
        Closes the remote file handle and the SFTP and SSH clients.
        """
        if self.file is not None:
            self.file.close()
            self.file = None
        if self.sftp_client is not None:
            self.sftp_client.close()
            self.sftp_client = None
        if self.ssh_client is not None:
            self.ssh_client.close()
            self.ssh_client = None

    def ssh(self):
        if self.ssh_client is None:
            self.ssh_client = get_ssh_client(self.sftp_uri, **self.ssh_options)
        return self.ssh_client

    def sftp(self):
        if self.sftp_client is None:
            self.sftp_client = SFTPClient.from_transport(self.ssh().get_transport())
        return self.sftp_client

    def ssh_exec(self, cmd):
        (stdin, stdout, stderr) = self.ssh().exec_command(cmd)
        return stdout.readline().rstrip('\n')

    def open(self):
        try:
            self.file = self.sftp().open(self.sftp_uri.path, mode='rb')
            return self.file
        except IOError as e:
            raise BinarySourceNotFoundError(str(e)) from e

    def mimetype(self):
        if self._mimetype is None:
            self._mimetype = self.ssh_exec(f'file --mime-type -F "" "{self.sftp_uri.path}"').split()[1]
        return self._mimetype

    def digest(self):
        sha1sum = self.ssh_exec(f'sha1sum "{self.sftp_uri.path}"').split()[0]
        return 'sha1=' + sha1sum

    def exists(self):
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
