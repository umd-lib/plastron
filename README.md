# Plastron

Utility for batch operations on a Fedora 4 repository.

## Installation

Requires Python 3.6+

### Install from GitHub

```bash
pip install git+https://github.com/umd-lib/plastron.git
```

To install a particular tag, branch, or commit, append the desired identifier
after an `@`:

```bash
# installs version tagged 3.5.1
pip install git+https://github.com/umd-lib/plastron.git@3.5.1

# installs from the develop branch
pip install git+https://github.com/umd-lib/plastron.git@develop

# installs from commit a2b398648266f57d96a94d90392da71a47f4d0aa
pip install git+https://github.com/umd-lib/plastron.git@a2b398648266f57d96a94d90392da71a47f4d0aa
```

### Installing Python 3 with pyenv (Optional)

If you don't already have a Python 3 environment, or would like to install
Plastron into its own isolated environment, a very convenient way to do this is
to use the [pyenv] Python version manager.

See these instructions for [installing pyenv](https://github.com/pyenv/pyenv#installation),
then run the following:

```bash
# install Python 3.6.12
pyenv install 3.6.12

# create a new virtual environment based on 3.6.12 for Plastron
pyenv virtualenv 3.6.12 plastron

# switch to that environment in your current shell
pyenv shell plastron

# or for the current directory
pyenv shell local plastron
```

### Installation for development

To install Plastron in [development mode], do the following:

```bash
git clone git@github.com:umd-lib/plastron.git
cd plastron
pip install -e '.[dev,test]'
```

This allows for in-place editing of Plastron's source code in the git
repository (i.e., it is not locked away in a Python site-packages directory
structure).

### Testing

Plastron uses the [pytest] test framework for its [tests](tests).

```bash
pytest
```

See the [testing documentation](docs/testing.md) for more information.

## Running

* [Command-line client](docs/cli.md) ([plastron.cli](plastron/cli.py))
* [Server](docs/daemon.md) ([plastron.daemon](plastron/daemon.py))

## Architecture

Plastron is designed in a modular fashion. Its major components are:

* Fedora repository REST API client ([http.py](plastron/http.py))
* RDF-to-Python object modeling ([rdf.py](plastron/rdf.py), [content models](plastron/models))
* Runnable commands (e.g., batch loading, exporting) ([command modules](plastron/commands))
* Data handlers for batch ingest formats ([handler modules](plastron/handlers))
* Entrypoint interfaces (currently a command-line tool and
  a daemon) ([cli.py](plastron/cli.py), [daemon.py](plastron/daemon.py))

The intent is that the runnable commands be useful units of work that can be
called interchangeably from any of the entrypoint interfaces, or be directly
included and called via import into other Python code.

## Name

> The plastron is the nearly flat part of the shell structure of a turtle,
> what one would call the belly or ventral surface of the shell.

Source: [Wikipedia](https://en.wikipedia.org/wiki/Turtle_shell#Plastron)

## License

See the [LICENSE](LICENSE.md) file for license rights and limitations (Apache 2.0).

[pyenv]: https://github.com/pyenv/pyenv
[development mode]: https://packaging.python.org/tutorials/installing-packages/#installing-from-vcs
[pytest]: https://pypi.org/project/pytest/
