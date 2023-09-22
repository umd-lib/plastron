# Plastron

Tools for working with a Fedora 4 repository.

## Architecture

Plastron is composed of several distribution packages:

* **[plastron-client](plastron-client)**: The Fedora repository API client
* **[plastron-rdf](plastron-rdf)**: RDF-to-Python property mapping
* **[plastron-models](plastron-models)**: Content models, CSV
  serialization
* **[plastron-repo](plastron-repo)**: Repository operations and structural
  models (LDP, PCDM, Web Annotations, etc.)
* **[plastron-cli](plastron-cli)**: Command-line tool. Also includes the
  handler classes for the `load` command
* **[plastron-stomp](plastron-stomp)**: STOMP daemon for handling
  asynchronous operations
* **[plastron-web](plastron-web)**: Web application for handling
  synchronous operations
* **[plastron-utils](plastron-utils)**: Miscellaneous utilities

The intent is that these distribution packages are independently useful,
either as tools that can be run or libraries to be included in other projects.

## Installation

Requires Python 3.8+

This repository includes a [.python-version](.python-version) file. If you are
using a tool like [pyenv] to manage your Python versions, it will select
an installed Python 3.8 for you.

### Install for development

To install Plastron in [development mode], do the following:

```bash
git clone git@github.com:umd-lib/plastron.git
cd plastron
python -m venv .venv
source .venv/bin/activate
pip install \
    -e './plastron-utils[test]' \
    -e './plastron-client[test]' \
    -e './plastron-rdf[test]' \
    -e './plastron-models[test]' \
    -e './plastron-repo[test]' \
    -e './plastron-web[test]' \
    -e './plastron-stomp[test]' \
    -e './plastron-cli[test]'
```

This allows for in-place editing of Plastron's source code in the git
repository (i.e., it is not locked away in a Python site-packages directory
structure).

### Testing

Plastron uses the [pytest] test framework for its tests.

```bash
pytest
```

See the [testing documentation](docs/testing.md) for more
information.

## Running

* [Command-line client](plastron-cli/docs/cli.md)
* [Server](plastron-stomp/docs/daemon.md)

## Name

> The plastron is the nearly flat part of the shell structure of a turtle,
> what one would call the belly or ventral surface of the shell.

Source: [Wikipedia](https://en.wikipedia.org/wiki/Turtle_shell#Plastron)

## License

See the [LICENSE](plastron-utils/LICENSE.md) file for license rights and
limitations (Apache 2.0).

[development mode]: https://packaging.python.org/tutorials/installing-packages/#installing-from-vcs
[pytest]: https://pypi.org/project/pytest/
[pyenv]: https://github.com/pyenv/pyenv
