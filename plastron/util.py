import csv
import logging
import os
import re
import shutil
import sys
import urllib
from argparse import ArgumentTypeError
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlsplit

from paramiko import AutoAddPolicy, SSHClient, SSHException
from paramiko.config import SSH_PORT
from rdflib import URIRef
from rdflib.util import from_n3

from plastron import namespaces
from plastron.exceptions import FailureException, RESTAPIException
from plastron.http import Transaction
from plastron.namespaces import dcterms

logger = logging.getLogger(__name__)


def get_title_string(graph, separator='; '):
    return separator.join([t for t in graph.objects(predicate=dcterms.title)])


def parse_predicate_list(string, delimiter=','):
    if string is None:
        return None
    manager = namespaces.get_manager()
    return [from_n3(p, nsm=manager) for p in string.split(delimiter)]


def uri_or_curie(arg: str):
    if arg and (arg.startswith('http://') or arg.startswith('https://')):
        # looks like an absolute HTTP URI
        return URIRef(arg)
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


def datetimestamp(digits_only=True):
    now = str(datetime.utcnow().isoformat(timespec='seconds'))
    if digits_only:
        return re.sub(r'[^0-9]', '', now)
    else:
        return now


def envsubst(value, env=None):
    """
    Recursively replace ${VAR_NAME} placeholders in value with the values of the
    corresponding keys of env. If env is not given, it defaults to the environment
    variables in os.environ.

    Any placeholders that do not have a corresponding key in the env dictionary
    are left as is.

    :param value: Value to search for ${VAR_NAME} placeholders.
    :param env: Dictionary of values to use as replacements. If not given, defaults
        to os.environ.
    :return: If value is a string, the result of replacing ${VAR_NAME} with the
        corresponding value from env. If value is a list, returns a new list where each
        item in value replaced with the result of calling envsubst() on that item. If
        value is a dictionary, returns a new dictionary where each item value is replaced
        with the result of calling envsubst() on that value.
    """
    if env is None:
        env = os.environ
    if isinstance(value, str):
        if '${' in value:
            try:
                return value.replace('${', '{').format(**env)
            except KeyError as e:
                missing_key = str(e.args[0])
                logger.warning(f'Environment variable ${{{missing_key}}} not found')
                # for a missing key, just return the string without substitution
                return envsubst(value, {missing_key: f'${{{missing_key}}}', **env})
        else:
            return value
    elif isinstance(value, list):
        return [envsubst(v, env) for v in value]
    elif isinstance(value, dict):
        return {k: envsubst(v, env) for k, v in value.items()}
    else:
        return value


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
            if not self.repository.contains(uri):
                logger.warning(f'Resource {uri} is not contained within the repository {self.repository.endpoint}')
                continue
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
        self.filename = Path(filename)
        self.fieldnames = fieldnames
        self.keyfield = keyfield
        self.write_header = header
        self.item_keys = set()
        self.fh = None
        self._writer = None
        if self.exists():
            self.load_keys()

    def exists(self):
        return self.filename.is_file()

    def create(self):
        with self.filename.open(mode='w', buffering=1) as fh:
            writer = csv.DictWriter(fh, fieldnames=self.fieldnames)
            if self.write_header:
                writer.writeheader()

    def load_keys(self):
        for row in iter(self):
            self.item_keys.add(row[self.keyfield])

    def __iter__(self):
        try:
            with self.filename.open(mode='r', buffering=1) as fh:
                reader = csv.DictReader(fh)
                # check the validity of the map file data
                if not reader.fieldnames == self.fieldnames:
                    logger.warning(
                        f'Fieldnames in {self.filename} do not match expected fieldnames; '
                        f'expected: {self.fieldnames}; found: {reader.fieldnames}'
                    )
                # read the data from the existing file
                yield from reader
        except FileNotFoundError:
            # log file not found, so stop the iteration
            return

    @property
    def writer(self):
        if not self.exists():
            self.create()
        if self.fh is None:
            self.fh = self.filename.open(mode='a', buffering=1)
        if self._writer is None:
            self._writer = csv.DictWriter(self.fh, fieldnames=self.fieldnames)
        return self._writer

    def append(self, row):
        self.writer.writerow(row)
        self.item_keys.add(row[self.keyfield])

    def writerow(self, row):
        self.append(row)

    def __contains__(self, other):
        return other in self.item_keys

    def __len__(self):
        return len(self.item_keys)


class ItemLogError(Exception):
    pass
