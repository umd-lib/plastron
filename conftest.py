from os.path import abspath, dirname
from pathlib import Path

import plastron.validation.vocabularies

# See "plastron-models/README.md" for more information about this file.


def pytest_configure(config):
    # Set VOCABULARIES_DIR and VOCABULARIES to point to local test files,
    # so that tests run without making a network call, and to provide greater
    # flexibility in defining the vocabularies used for testing.
    #
    # A network call will still be made if a vocabulary is not specified in the
    # "VOCABULARIES" dictionary, because of the fallback behavior in the
    # "get_vocabulary" method of
    # plastron-models/src/plastron/validation/vocabularies/__init__.py
    plastron.validation.vocabularies.VOCABULARIES_DIR = Path(
        dirname(abspath(__file__)), 'plastron-models', 'tests', 'data', 'vocabularies'
    )
    plastron.validation.vocabularies.VOCABULARIES = {
        'http://purl.org/dc/dcmitype/': 'dcmitype.ttl',
        'http://vocab.lib.umd.edu/collection#': 'collection.ttl',
        'http://vocab.lib.umd.edu/form#': 'form.ttl',
        'http://vocab.lib.umd.edu/rightsStatement#': 'rightsStatement.ttl',
        'http://vocab.lib.umd.edu/set#': 'set.ttl',
        'http://vocab.lib.umd.edu/termsOfUse#': 'termsOfUse.ttl'
    }
