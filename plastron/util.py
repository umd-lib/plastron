import csv
import logging
import shutil
import sys
import urllib
from argparse import ArgumentTypeError
from os.path import isfile
from paramiko import AutoAddPolicy, SSHClient, SSHException
from paramiko.config import SSH_PORT
from plastron import namespaces
from plastron.exceptions import RESTAPIException, FailureException
from plastron.http import Transaction
from plastron.namespaces import dcterms
from rdflib import URIRef
from rdflib.util import from_n3
from tempfile import NamedTemporaryFile
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)


def get_title_string(graph, separator='; '):
    return separator.join([t for t in graph.objects(predicate=dcterms.title)])


def parse_predicate_list(string, delimiter=','):
    if string is None:
        return None
    manager = namespaces.get_manager()
    return [from_n3(p, nsm=manager) for p in string.split(delimiter)]


def uri_or_curie(arg):
    try:
        term = from_n3(arg, nsm=namespaces.get_manager())
    except KeyError:
        raise ArgumentTypeError(f'"{arg[:arg.index(":") + 1]}" is not a known prefix')
    if not isinstance(term, URIRef):
        raise ArgumentTypeError('must be a URI or CURIE')
    return term


def get_ssh_client(sftp_uri, **kwargs):
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
        raise FailureException(str(e)) from e


class ResourceList:
    def __init__(self, repository, uri_list=None, file=None, completed_file=None):
        self.repository = repository
        self.uri_list = uri_list
        self.file = file
        self.use_transaction = True
        if completed_file is not None:
            logger.info(f'Reading the completed items log from {completed_file}')
            # read the log of completed items
            fieldnames = ['uri', 'title', 'timestamp']
            try:
                self.completed = ItemLog(completed_file, fieldnames, 'uri')
                logger.info(f'Found {len(self.completed)} completed item(s)')
            except Exception as e:
                logger.error(f"Non-standard map file specified: {e}")
                raise FailureException()
        else:
            self.completed = None
        self.completed_buffer = None

    def get_uris(self):
        if self.file is not None:
            if self.file == '-':
                # special filename "-" means STDIN
                for line in sys.stdin:
                    yield line
            else:
                with open(self.file) as fh:
                    for line in fh:
                        yield line.rstrip()
        else:
            for uri in self.uri_list:
                yield uri

    def get_resources(self, traverse=None):
        for uri in self.get_uris():
            for resource, graph in self.repository.recursive_get(uri, traverse=traverse):
                yield resource, graph

    def process(self, method, use_transaction=True, traverse=None):
        self.use_transaction = use_transaction
        if traverse is not None:
            predicate_list = ', '.join(p.n3() for p in traverse)
            logger.info(f"{method.__name__} will traverse the following predicates: {predicate_list}")

        if use_transaction:
            # set up a temporary ItemLog that will be copied to the real item log upon completion of the transaction
            self.completed_buffer = ItemLog(
                NamedTemporaryFile().name,
                ['uri', 'title', 'timestamp'],
                'uri',
                header=False
            )
            with Transaction(self.repository, keep_alive=90) as transaction:
                for resource, graph in self.get_resources(traverse=traverse):
                    try:
                        method(resource, graph)
                    except RESTAPIException as e:
                        logger.error(f'{method.__name__} failed for {resource}: {e}: {e.response.text}')
                        # if anything fails while processing of the list of uris, attempt to
                        # rollback the transaction. Failures here will be caught by the main
                        # loop's exception handler and should trigger a system exit
                        try:
                            transaction.rollback()
                            logger.warning('Transaction rolled back.')
                            return False
                        except RESTAPIException:
                            logger.error('Unable to roll back transaction, aborting')
                            raise FailureException()
                transaction.commit()
                if self.completed and self.completed.filename:
                    shutil.copyfile(self.completed_buffer.filename, self.completed.filename)
                return True
        else:
            for resource, graph in self.get_resources(traverse=traverse):
                try:
                    method(resource, graph)
                except RESTAPIException as e:
                    logger.error(f'{method.__name__} failed for {resource}: {e}: {e.response.text}')
                    logger.warning(f'Continuing {method.__name__} with next item')
            return True

    def log_completed(self, uri, title, timestamp):
        if self.completed is not None:
            row = {'uri': uri, 'title': title, 'timestamp': timestamp}
            if self.use_transaction:
                self.completed_buffer.writerow(row)
            else:
                self.completed.writerow(row)


class ItemLog:
    def __init__(self, filename, fieldnames, keyfield, header=True):
        self.filename = filename
        self.fieldnames = fieldnames
        self.keyfield = keyfield
        self.item_keys = set()
        self.fh = None
        self.writer = None

        if not isfile(self.filename):
            with open(self.filename, 'w', 1) as fh:
                writer = csv.DictWriter(fh, fieldnames=self.fieldnames)
                if header:
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
