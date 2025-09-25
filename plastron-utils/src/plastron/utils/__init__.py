import logging
import os
import platform
import re
from argparse import ArgumentTypeError
from datetime import datetime
from typing import Mapping, Optional

from rdflib import URIRef
from rdflib.term import Node
from rdflib.util import from_n3

from plastron import namespaces

DEFAULT_LOGGING_OPTIONS = {
    'version': 1,
    'formatters': {
        'full': {
            'format': '%(levelname)s|%(asctime)s|%(threadName)s|%(name)s|%(message)s'
        },
        'messageonly': {
            'format': '%(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'messageonly',
            'stream': 'ext://sys.stderr'
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'full'
        }
    },
    'loggers': {
        '__main__': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
            'propagate': False
        },
        'plastron': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
            'propagate': False
        },
        # suppress logging output from paramiko by default
        'paramiko': {
            'propagate': False
        }
    },
    'root': {
        'level': 'DEBUG'
    }
}
logger = logging.getLogger(__name__)


def datetimestamp(digits_only: bool = True) -> str:
    """Returns a string containing the current UTC timestamp. By default, it
    is only digits (`20231117151827` vs. `2023-11-17T15:18:27`). If you want
    the full ISO 8601 representation, set `digits_only` to `True`.

    ```pycon
    >>> datetimestamp()
    '20231117152014'

    >>> datetimestamp(digits_only=False)
    '2023-11-17T15:20:57'
    ```
    """
    now = str(datetime.utcnow().isoformat(timespec='seconds'))
    if digits_only:
        return re.sub(r'[^0-9]', '', now)
    else:
        return now


def envsubst(value: str | list | dict, env: Mapping[str, str] = None) -> str | list | dict:
    """
    Recursively replace `${VAR_NAME}` placeholders in value with the values of the
    corresponding keys of env. If env is not given, it defaults to the environment
    variables in os.environ.

    Any placeholders that do not have a corresponding key in the env dictionary
    are left as is.

    :param value: String, list, or dictionary to search for `${VAR_NAME}` placeholders.
    :param env: Dictionary of values to use as replacements. If not given, defaults
        to `os.environ`.
    :return: If `value` is a string, returns the result of replacing `${VAR_NAME}` with the
        corresponding `value` from env. If `value` is a list, returns a new list where each
        item in `value` replaced with the result of calling `envsubst()` on that item. If
        `value` is a dictionary, returns a new dictionary where each item in `value` is replaced
        with the result of calling `envsubst()` on that item.
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


def check_python_version():
    # check Python version
    major, minor, patch = (int(v) for v in platform.python_version_tuple())
    if minor < 8:
        logger.warning(
            f'You appear to be running Python {platform.python_version()}. '
            'Upgrading to Python 3.8+ is STRONGLY recommended.'
        )


def strtobool(val: str) -> int:
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.

    This implementation is copied from distutils/util.py in Python 3.10.4,
    in order to retain this functionality once distutils is removed in
    Python 3.12. See also https://peps.python.org/pep-0632/#migration-advice
    and https://docs.python.org/3.10/whatsnew/3.10.html#distutils-deprecated.

    Note that even though this function is named `strtobool`, it actually
    returns an integer. This is copied directly from the distutils module.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


def uri_or_curie(arg: str) -> URIRef:
    """Convert a string to a URIRef. If it begins with either `http://`
    or `https://`, treat it as an absolute HTTP URI. Otherwise, try to
    parse it as a CURIE (e.g., "dcterms:title") and return the expanded
    URI. If the prefix is not recognized, or if `from_n3()` returns anything
    but a URIRef, raises `ArgumentTypeError`."""
    if arg and (arg.startswith('http://') or arg.startswith('https://')):
        # looks like an absolute HTTP URI
        return URIRef(arg)
    try:
        term = from_n3(arg, nsm=namespaces.get_manager())
    except KeyError:
        raise ArgumentTypeError(f'"{arg[:arg.index(":") + 1]}" is not a known prefix')
    if not isinstance(term, URIRef):
        raise ArgumentTypeError(f'"{arg}" must be a URI or CURIE')
    return term


def parse_predicate_list(string: str, delimiter: str = ',') -> Optional[list[Node]]:
    if string is None:
        return None
    manager = namespaces.get_manager()
    return [from_n3(p, nsm=manager) for p in string.split(delimiter)]
