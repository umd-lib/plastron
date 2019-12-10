import csv
import hashlib
import logging
import mimetypes
import sys
from os.path import basename, isfile
from paramiko import SSHClient, SFTPClient
from plastron import namespaces
from plastron.exceptions import RESTAPIException, FailureException
from plastron.http import Transaction
from plastron.namespaces import dcterms, ebucore
from rdflib import URIRef
from rdflib.util import from_n3

logger = logging.getLogger(__name__)


def get_title_string(graph, separator='; '):
    return separator.join([t for t in graph.objects(predicate=dcterms.title)])


def parse_predicate_list(string, delimiter=','):
    manager = namespaces.get_manager()
    return [from_n3(p, nsm=manager) for p in string.split(delimiter)]


def process_resources(method, repository, uri_list=None, file=None, recursive=None, use_transaction=True):
    if recursive is not None:
        predicates = parse_predicate_list(recursive)
        predicate_list = ', '.join(p.n3() for p in predicates)
        logger.info(f"{method.__name__} will traverse the following predicates: {predicate_list}")
    else:
        predicates = []

    if file is not None:
        if file == '-':
            # special filename "-" means STDIN
            uri_list = sys.stdin
        else:
            with open(file) as fh:
                uri_list = [ line.rstrip() for line in fh ]

    # closure for processing the list of items
    def process_list():
        for uri in uri_list:
            for target_uri, graph in repository.recursive_get(uri, traverse=predicates):
                method(target_uri, graph)

    if use_transaction:
        try:
            with Transaction(repository, keep_alive=90) as txn:
                try:
                    process_list()
                    txn.commit()

                except RESTAPIException as e:
                    # if anything fails while processing of the list of uris, attempt to
                    # rollback the transaction. Failures here will be caught by the main
                    # loop's exception handler and should trigger a system exit
                    logger.error(f'Failed: {e}')
                    txn.rollback()
                    logger.warning('Transaction rolled back.')

        except RESTAPIException:
            logger.error('Unable to roll back transaction, aborting')
            raise FailureException()

    else:
        process_list()


def print_header():
    """Common header formatting."""
    title = '|     PLASTRON     |'
    bar = '+' + '=' * (len(title) - 2) + '+'
    spacer = '|' + ' ' * (len(title) - 2) + '|'
    print('\n'.join(['', bar, spacer, title, spacer, bar, '']))


def print_footer():
    """Report success or failure and resources created."""
    print('\nScript complete. Goodbye!\n')


class ItemLog:
    def __init__(self, filename, fieldnames, keyfield):
        self.filename = filename
        self.fieldnames = fieldnames
        self.keyfield = keyfield
        self.item_keys = set()
        self.fh = None
        self.writer = None

        if not isfile(self.filename):
            with open(self.filename, 'w', 1) as fh:
                writer = csv.DictWriter(fh, fieldnames=self.fieldnames)
                writer.writeheader()
        else:
            with open(self.filename, 'r', 1) as fh:
                reader = csv.DictReader(fh)

                # check the validity of the map file data
                if not reader.fieldnames == fieldnames:
                    raise Exception('Fieldnames in {0} do not match expected fieldnames'.format(filename))

                # read the data from the existing file
                for row in reader:
                    self.item_keys.add(row[self.keyfield])

    def get_writer(self):
        if self.fh is None:
            self.fh = open(self.filename, 'a', 1)
        if self.writer is None:
            self.writer = csv.DictWriter(self.fh, fieldnames=self.fieldnames)
        return self.writer

    def writerow(self, row):
        self.get_writer().writerow(row)
        self.item_keys.add(row[self.keyfield])

    def __contains__(self, other):
        return other in self.item_keys

    def __len__(self):
        return len(self.item_keys)

    def __del__(self):
        if self.fh is not None:
            self.fh.close()


class BinarySource(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)


class LocalFile(BinarySource):
    def __init__(self, localpath, mimetype=None, filename=None):
        super().__init__()
        if mimetype is None:
            mimetype = mimetypes.guess_type(localpath)[0]
        self._mimetype = mimetype
        self.localpath = localpath
        self.filename = filename if filename is not None else basename(localpath)

    def data(self):
        return open(self.localpath, 'rb')

    def mimetype(self):
        return self._mimetype

    # generate SHA1 checksum on a file
    def digest(self):
        sha1 = hashlib.sha1()
        with self.data() as stream:
            for block in stream:
                sha1.update(block)
        return 'sha1=' + sha1.hexdigest()


class RepositoryFile(BinarySource):
    def __init__(self, repo, file_uri):
        super().__init__()
        file_uri = URIRef(file_uri)
        head_res = repo.head(file_uri)
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
        return self.sftp().open(self.remotepath, mode='rb')

    def mimetype(self):
        if self._mimetype is None:
            self._mimetype = self.ssh_exec(f'file --mime-type -F "" "{self.remotepath}"').split()[1]
        return self._mimetype

    def digest(self):
        sha1sum = self.ssh_exec(f'sha1sum "{self.remotepath}"').split()[0]
        return 'sha1=' + sha1sum
