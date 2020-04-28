import hashlib
import logging
import mimetypes
import zipfile
from os.path import basename
from paramiko import SSHClient, SFTPClient
from plastron.exceptions import BinarySourceNotFoundError, RESTAPIException
from plastron.namespaces import dcterms, ebucore
from rdflib import URIRef


class BinarySource(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    def data(self):
        raise NotImplementedError()

    def mimetype(self):
        raise NotImplementedError()

    # generate SHA1 checksum on a file
    def digest(self):
        sha1 = hashlib.sha1()
        with self.data() as stream:
            for block in stream:
                sha1.update(block)
        return 'sha1=' + sha1.hexdigest()


class LocalFile(BinarySource):
    def __init__(self, localpath, mimetype=None, filename=None):
        super().__init__()
        if mimetype is None:
            mimetype = mimetypes.guess_type(localpath)[0]
        self._mimetype = mimetype
        self.localpath = localpath
        self.filename = filename if filename is not None else basename(localpath)

    def data(self):
        try:
            return open(self.localpath, 'rb')
        except FileNotFoundError as e:
            raise BinarySourceNotFoundError(str(e)) from e

    def mimetype(self):
        return self._mimetype


class RepositoryFile(BinarySource):
    def __init__(self, repo, file_uri):
        super().__init__()
        file_uri = URIRef(file_uri)
        head_res = repo.head(file_uri)

        if head_res.status_code == 404:
            raise BinarySourceNotFoundError(f'{head_res.status_code} {head_res.reason}: {file_uri}')
        if head_res.status_code != 200:
            raise RESTAPIException(head_res)

        if 'describedby' in head_res.links:
            rdf_uri = head_res.links['describedby']['url']
            file_graph = repo.get_graph(rdf_uri)

            self.file_uri = file_uri
            self.repo = repo

            self.title = file_graph.value(subject=file_uri, predicate=dcterms.title)
            self._mimetype = file_graph.value(subject=file_uri, predicate=ebucore.hasMimeType)
            self.filename = file_graph.value(subject=file_uri, predicate=ebucore.filename)
            self.file_graph = file_graph
            self.metadata_uri = rdf_uri
        else:
            raise Exception("No metadata for resource")

    def mimetype(self):
        return self._mimetype

    def data(self):
        return self.repo.get(self.file_uri, stream=True).raw


class RemoteFile(BinarySource):
    def __init__(self, host, remotepath, mimetype=None):
        super().__init__()
        self.ssh_client = None
        self.sftp_client = None
        self.host = host
        self.remotepath = remotepath
        self.filename = basename(remotepath)
        self._mimetype = mimetype

    def __del__(self):
        # cleanup the SFTP and SSH clients
        if self.sftp_client is not None:
            self.sftp_client.close()
        if self.ssh_client is not None:
            self.ssh_client.close()

    def ssh(self):
        if self.ssh_client is None:
            self.ssh_client = SSHClient()
            self.ssh_client.load_system_host_keys()
            self.ssh_client.connect(self.host)
        return self.ssh_client

    def sftp(self):
        if self.sftp_client is None:
            self.sftp_client = SFTPClient.from_transport(self.ssh().get_transport())
        return self.sftp_client

    def ssh_exec(self, cmd):
        (stdin, stdout, stderr) = self.ssh().exec_command(cmd)
        return stdout.readline().rstrip('\n')

    def data(self):
        try:
            return self.sftp().open(self.remotepath, mode='rb')
        except IOError as e:
            raise BinarySourceNotFoundError(str(e)) from e

    def mimetype(self):
        if self._mimetype is None:
            self._mimetype = self.ssh_exec(f'file --mime-type -F "" "{self.remotepath}"').split()[1]
        return self._mimetype

    def digest(self):
        sha1sum = self.ssh_exec(f'sha1sum "{self.remotepath}"').split()[0]
        return 'sha1=' + sha1sum


class ZipFile(BinarySource):
    def __init__(self, zip_file, path, mimetype=None):
        super().__init__()
        if isinstance(zip_file, zipfile.ZipFile):
            self.zip_filename = zip_file.filename
            self.zip_file = zip_file
        else:
            self.zip_filename = zip_file
            self.zip_file = None
            try:
                self.zip_file = zipfile.ZipFile(zip_file)
            except FileNotFoundError as e:
                raise BinarySourceNotFoundError(f'Zip file {zip_file} not found') from e
        self.path = path
        self.filename = basename(path)
        if mimetype is None:
            mimetype = mimetypes.guess_type(path)[0]
        self._mimetype = mimetype

    def data(self):
        try:
            return self.zip_file.open(self.path, 'r')
        except KeyError as e:
            raise BinarySourceNotFoundError(f"'{self.path}' not found in file '{self.zip_filename}'") from e

    def mimetype(self):
        return self._mimetype