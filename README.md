# Plastron

Tools for working with a Fedora 4 repository.

## Architecture

Plastron is composed of several distribution packages:

* **[plastron-client](plastron-client)**: The Fedora repository API client
* **[plastron-models](plastron-models)**: RDF-to-Python object modeling, CSV 
  serialization
* **[plastron-legacy](plastron-legacy)**: Everything else (currently):
  * Runnable commands (e.g., batch loading, exporting)
  * Data handlers for batch ingest formats
  * Entrypoint interfaces (currently a command-line tool and a daemon)
* **plastron-cli** (planned)
* **plastron-stomp** (planned)
* **plastron-web** (planned)

The intent is that these distribution packages are independently useful, 
either as tools that can be run or libraries to be included in other projects.

## Installation

Requires Python 3.8+

### Install from GitHub

```bash
pip install git+https://github.com/umd-lib/plastron
```

### Install for development

To install Plastron in [development mode], do the following:

```bash
git clone git@github.com:umd-lib/plastron.git
cd plastron
python -m venv .venv
source .venv/bin/activate
pip install -e \
    plastron-client[test] \
    plastron-models[test] \
    plastron-legacy[test]
```

This allows for in-place editing of Plastron's source code in the git
repository (i.e., it is not locked away in a Python site-packages directory
structure).

### Testing

Plastron uses the [pytest] test framework for its tests.
(plastron-legacy/tests).

```bash
pytest
```

See the [testing documentation](plastron-legacy/docs/testing.md) for more
information.

## Running

* [Command-line client](plastron-legacy/docs/cli.md)
* [Server](plastron-legacy/docs/daemon.md)

## Name

> The plastron is the nearly flat part of the shell structure of a turtle,
> what one would call the belly or ventral surface of the shell.

Source: [Wikipedia](https://en.wikipedia.org/wiki/Turtle_shell#Plastron)

## License

See the [LICENSE](plastron-legacy/LICENSE.md) file for license rights and
limitations (Apache 2.0).

[development mode]: https://packaging.python.org/tutorials/installing-packages/#installing-from-vcs
[pytest]: https://pypi.org/project/pytest/
